package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/teamboard/internal/tenantdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	alphaTenant = uuid.MustParse("10000000-0000-0000-0000-000000000001")
	demoProject = uuid.MustParse("11990000-0000-0000-0000-000000000001")
)

// testStore подключается под рабочей ролью приложения (не под владельцем
// базы), поэтому все запросы проходят через политику изоляции арендаторов —
// ровно как в рабочем коде.
func testStore(t *testing.T) (*tenantdb.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("APP_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://teamboard_app:teamboard_app@localhost:55434/teamboard?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return tenantdb.New(pool), pool
}

// resetDemo приводит demo-проект к исходному состоянию «две открытые
// задачи при лимите три»; тесты пакета не зависят от порядка выполнения
// и от результатов предыдущих прогонов.
func resetDemo(t *testing.T, store *tenantdb.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			"DELETE FROM tasks WHERE project_id=$1", demoProject,
		); err != nil {
			return err
		}
		for i := 1; i <= 2; i++ {
			_, err := tx.Exec(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,$4)`,
				alphaTenant, uuid.New(), demoProject,
				fmt.Sprintf("existing %d", i))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// demoEnv — контракт тестов пакета: перед тестом demo-проект приведён к
// исходному состоянию, после теста — тоже. Без восстановления финальное
// число задач Alpha зависело бы от того, какие тесты выполнялись, и
// ломало бы интеграционные проверки соседнего пакета tenantdb,
// ожидающие точный размер датасета арендатора.
func demoEnv(t *testing.T, store *tenantdb.Store) {
	t.Helper()
	resetDemo(t, store)
	t.Cleanup(func() { resetDemo(t, store) })
}

func openCount(t *testing.T, store *tenantdb.Store) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var count int
	err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
SELECT count(*) FROM tasks
WHERE project_id=$1 AND status='open'`, demoProject).Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

// TestReadCommittedAllowsWriteSkew — тест-демонстрация (РХД-03, шаг 4):
// PASS означает «аномалия воспроизведена», а не «реализация корректна».
// Барьер ready/release гарантирует, что обе транзакции прочитали счётчик
// 2/3 до того, как любая из них выполнила вставку. На READ COMMITTED
// каждая честно проверила условие в своём снимке, вставила свою строку
// (конфликта блокировок нет — строки разные) — и в проекте оказывается
// четыре открытые задачи при лимите три.
func TestReadCommittedAllowsWriteSkew(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup

	for i := range 2 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			errs <- store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
				var count, limit int
				err := tx.QueryRow(ctx, `
SELECT count(*), p.open_task_limit
FROM projects p
LEFT JOIN tasks t
  ON t.tenant_id=p.tenant_id AND t.project_id=p.id AND t.status='open'
WHERE p.id=$1
GROUP BY p.open_task_limit`, demoProject).Scan(&count, &limit)
				if err != nil {
					return err
				}
				if count >= limit {
					return fmt.Errorf("limit reached")
				}
				ready <- struct{}{}
				<-release
				_, err = tx.Exec(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,$4)`, alphaTenant, uuid.New(), demoProject,
					fmt.Sprintf("racer %d", worker))
				return err
			})
		}(i)
	}
	<-ready
	<-ready
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected transaction error: %v", err)
		}
	}

	if got := openCount(t, store); got != 4 {
		t.Fatalf("open task count=%d, want demonstrated anomaly 4", got)
	}
}

var errLimitReached = errors.New("open task limit reached")

// TestSerializableClosesWriteSkew — то же расписание, но уровень
// SERIALIZABLE и повтор транзакции целиком через RunSerializable.
// SSI обнаруживает, что решение одной из транзакций опиралось на данные,
// изменённые конкурентной фиксацией, и отменяет её с SQLSTATE 40001;
// повторная попытка читает уже 3/3 и возвращает законный отказ по
// бизнес-правилу. Итог: ровно один успех, ровно один типизированный
// отказ, три открытые задачи при лимите три.
func TestSerializableClosesWriteSkew(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	var retries [2]atomic.Int32

	for i := range 2 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			firstAttempt := true
			errs <- RunSerializable(ctx, store, alphaTenant, func(tx pgx.Tx) error {
				if firstAttempt {
					// Барьер срабатывает только в первой попытке:
					// иначе повтор блокировал бы сам себя на уже
					// закрытом release.
					firstAttempt = false
				} else {
					// Повторная попытка после 40001: замыкание вызвано
					// заново, с нового BEGIN.
					retries[worker].Add(1)
				}
				var count, limit int
				err := tx.QueryRow(ctx, `
SELECT count(*), p.open_task_limit
FROM projects p
LEFT JOIN tasks t
  ON t.tenant_id=p.tenant_id AND t.project_id=p.id AND t.status='open'
WHERE p.id=$1
GROUP BY p.open_task_limit`, demoProject).Scan(&count, &limit)
				if err != nil {
					return err
				}
				if count >= limit {
					return errLimitReached
				}
				ready <- struct{}{}
				<-release
				_, err = tx.Exec(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,$4)`, alphaTenant, uuid.New(), demoProject,
					fmt.Sprintf("serial racer %d", worker))
				return err
			})
		}(i)
	}
	<-ready
	<-ready
	close(release)
	wg.Wait()
	close(errs)

	var successes, rejected int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, errLimitReached):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("successes=%d rejected=%d, want 1 and 1", successes, rejected)
	}
	if got := openCount(t, store); got != 3 {
		t.Fatalf("open task count=%d, want 3", got)
	}
	totalRetries := retries[0].Load() + retries[1].Load()
	t.Logf("write skew closed: successes=%d rejected=%d open=%d; retries after 40001: worker0=%d worker1=%d total=%d",
		successes, rejected, openCount(t, store),
		retries[0].Load(), retries[1].Load(), totalRetries)
}
