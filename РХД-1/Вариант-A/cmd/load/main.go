package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const query = `
SELECT id, title, created_at
FROM tasks
WHERE tenant_id = $1 AND project_id = $2 AND status = 'open'
ORDER BY created_at DESC, id
LIMIT 50`

type job struct {
	tenant  string
	project string
}

var targets = []job{
	// Вариант А: «горячий» проект Alpha Core (targets[0]) и 10 прочих
	// (9 дополнительных проектов Alpha из 002b плюс проект Beta).
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000001"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000002"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000003"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000004"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000005"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000006"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000007"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000008"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-000000000009"},
	{"10000000-0000-0000-0000-000000000001", "11000000-0000-0000-0000-00000000000a"},
	{"20000000-0000-0000-0000-000000000002", "22000000-0000-0000-0000-000000000002"},
}

// pickTarget детерминированно выбирает цель по номеру задания: из каждых
// пяти операций четыре (i%5 < 4) приходятся на «горячий» проект targets[0],
// каждая пятая — циклически по прочим десяти проектам. Повторные опыты
// воспроизводят то же распределение 80/20.
func pickTarget(i int) job {
	if i%5 < 4 {
		return targets[0]
	}
	others := targets[1:]
	return others[(i/5)%len(others)]
}

type result struct {
	duration time.Duration
	err      error
}

func main() {
	workers := flag.Int("workers", 8, "число рабочих горутин: 1..32")
	poolSize := flag.Int("pool", 0, "соединения: 1..workers; 0 означает workers")
	requests := flag.Int("requests", 500, "число запросов: 1..1000000")
	timeout := flag.Duration("timeout", 20*time.Second, "предельное время измеряемой фазы")
	flag.Parse()

	connections := *poolSize
	if connections == 0 {
		connections = *workers
	}
	if *workers < 1 || *workers > 32 || connections < 1 ||
		connections > *workers || *requests < 1 || *requests > 1_000_000 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "требуются workers=1..32, pool=0..workers, requests=1..1000000, timeout>0")
		os.Exit(2)
	}
	// Все defer внутри run выполнятся до выхода из процесса.
	if err := run(*workers, connections, *requests, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "опыт неуспешен:", err)
		os.Exit(1)
	}
}

func run(workers, connections, requests int, timeout time.Duration) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/teamboard?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("строка подключения: %w", err)
	}
	cfg.MaxConns = int32(connections)

	// Подготовка имеет свой срок и не расходует бюджет измерения.
	prepCtx, prepCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer prepCancel()
	pool, err := pgxpool.NewWithConfig(prepCtx, cfg)
	if err != nil {
		return fmt.Errorf("создание пула: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(prepCtx); err != nil {
		return fmt.Errorf("база недоступна: %w", err)
	}
	if err := checkData(prepCtx, pool); err != nil {
		return err
	}
	if err := warmPool(prepCtx, pool, connections); err != nil {
		return fmt.Errorf("прогрев: %w", err)
	}
	prepCancel()

	// Отсчёт начинается после проверки данных и прогрева всех соединений.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	started := time.Now()
	jobs := make(chan job, workers)
	results := make(chan result, workers)
	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		defer close(jobs)
		for i := range requests {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- pickTarget(i):
			}
		}
	}()

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j, ok := <-jobs:
					if !ok || ctx.Err() != nil {
						return
					}
					t := time.Now()
					err := readTasks(ctx, pool, j)
					// Начатая операция всегда учитывается, даже при отмене.
					// Главная горутина читает результаты до закрытия канала.
					results <- result{duration: time.Since(t), err: err}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		<-producerDone
		close(results)
	}()

	values := make([]time.Duration, 0, requests)
	attempted, failures := 0, 0
	var total time.Duration
	var firstErr error
	for r := range results {
		attempted++
		if r.err != nil {
			failures++
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		values = append(values, r.duration)
		total += r.duration
	}
	elapsed := time.Since(started)
	deadlineErr := ctx.Err()
	successful := len(values)
	fmt.Printf("planned=%d attempted=%d successful=%d failures=%d not_started=%d workers=%d pool=%d elapsed=%s successful_throughput=%.1f op/s\n",
		requests, attempted, successful, failures, requests-attempted,
		workers, connections, elapsed, float64(successful)/elapsed.Seconds())
	if successful > 0 {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		fmt.Printf("successful_latency: mean=%s p50=%s p95=%s p99=%s max=%s\n",
			total/time.Duration(successful), percentile(values, 0.50),
			percentile(values, 0.95), percentile(values, 0.99), values[successful-1])
	} else {
		fmt.Println("successful_latency: нет успешных наблюдений")
	}
	if deadlineErr != nil {
		return fmt.Errorf("истёк срок измерения: %w", deadlineErr)
	}
	if failures > 0 {
		return fmt.Errorf("%d ошибок; первая: %w", failures, firstErr)
	}
	if attempted != requests {
		return fmt.Errorf("выполнено %d из %d запросов", attempted, requests)
	}
	return nil
}

func checkData(ctx context.Context, pool *pgxpool.Pool) error {
	for _, j := range targets {
		var all, open, done int
		err := pool.QueryRow(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE status = 'open'),
       count(*) FILTER (WHERE status = 'done')
FROM tasks WHERE tenant_id = $1 AND project_id = $2`,
			j.tenant, j.project).Scan(&all, &open, &done)
		if err != nil {
			return fmt.Errorf("проверка данных: %w", err)
		}
		// Вариант А: ожидаемые количества для всех 11 целей —
		// по 10 000 задач на проект (7 500 open / 2 500 done).
		if all != 10000 || open != 7500 || done != 2500 {
			return fmt.Errorf("проект %s: получено total/open/done=%d/%d/%d, ожидалось 10000/7500/2500; проверьте наполнение и DATABASE_URL",
				j.project, all, open, done)
		}
	}
	return nil
}

func warmPool(ctx context.Context, pool *pgxpool.Pool, count int) error {
	held := make([]*pgxpool.Conn, 0, count)
	defer func() {
		for _, conn := range held {
			conn.Release()
		}
	}()
	// Удерживаем уже полученные соединения, чтобы Acquire открыл следующее.
	// Иначе цикл мог бы прогревать одно и то же соединение.
	for range count {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		held = append(held, conn)
	}
	for _, conn := range held {
		for _, j := range targets {
			if err := readTasks(ctx, conn, j); err != nil {
				return err
			}
		}
	}
	return nil
}

// Общий метод позволяет одинаково читать через пул и конкретное соединение.
type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func readTasks(ctx context.Context, db querier, j job) error {
	rows, err := db.Query(ctx, query, j.tenant, j.project)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, title string
		var created time.Time
		if err := rows.Scan(&id, &title, &created); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 50 {
		return fmt.Errorf("проект %s: получено %d строк, ожидалось 50", j.project, count)
	}
	return nil
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(p*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
