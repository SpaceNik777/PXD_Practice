// Вариант А (РХД-03): оптимистическое редактирование и идемпотентное
// завершение задачи. Все барьеры — детерминированные каналы
// ready/release, без sleep-«проверок».
package command

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"example.com/teamboard/internal/tenantdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// seedTask вставляет в demo-проект задачу с известным идентификатором
// и возвращает её начальную версию.
func seedTask(t *testing.T, store *tenantdb.Store, taskID uuid.UUID, title string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var version int64
	err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,$4)
RETURNING version`,
			alphaTenant, taskID, demoProject, title).Scan(&version)
	})
	if err != nil {
		t.Fatal(err)
	}
	return version
}

// taskState возвращает статус, версию и заголовок задачи (0 строк — нули).
func taskState(t *testing.T, store *tenantdb.Store, taskID uuid.UUID) (status string, version int64, title string, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
SELECT status, version, title, (SELECT count(*) FROM tasks WHERE id=$1)
FROM tasks WHERE tenant_id=$2 AND id=$1`,
			taskID, alphaTenant).Scan(&status, &version, &title, &count)
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

// TestOptimisticVersionConflict — два клиента прочитали одну и ту же
// версию V (барьер между чтением и записью), оба пишут разные заголовки
// с expectedVersion=V. Ровно один успех (версия V+1), второй —
// ErrVersionConflict; предметные данные консистентны: одна запись,
// одна версия, заголовок победителя.
func TestOptimisticVersionConflict(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)
	service := NewService(store)

	taskID := uuid.MustParse("a3900000-0000-0000-0000-000000000001")
	v0 := seedTask(t, store, taskID, "shared draft")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	type outcome struct {
		newVersion int64
		err        error
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup

	for worker := range 2 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			// Фаза чтения: каждый клиент в своей транзакции читает
			// версию и «уходит думать» (форма открыта минутами).
			var seen int64
			err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
				return tx.QueryRow(ctx,
					"SELECT version FROM tasks WHERE tenant_id=$1 AND id=$2",
					alphaTenant, taskID).Scan(&seen)
			})
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			ready <- struct{}{}
			<-release
			newVersion, err := service.UpdateTaskTitle(ctx, alphaTenant,
				taskID, seen, "title from client "+string(rune('A'+worker)))
			outcomes <- outcome{newVersion: newVersion, err: err}
		}(worker)
	}
	<-ready
	<-ready
	close(release)
	wg.Wait()
	close(outcomes)

	successes, conflicts := 0, 0
	var winnerVersion int64
	for o := range outcomes {
		switch {
		case o.err == nil:
			successes++
			winnerVersion = o.newVersion
		case errors.Is(o.err, ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", o.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d, want 1 and 1", successes, conflicts)
	}
	if winnerVersion != v0+1 {
		t.Fatalf("winner version=%d, want %d", winnerVersion, v0+1)
	}

	status, version, title, count := taskState(t, store, taskID)
	if count != 1 || status != "open" || version != v0+1 {
		t.Fatalf("task count=%d status=%q version=%d, want 1 open %d",
			count, status, version, v0+1)
	}
	if title != "title from client A" && title != "title from client B" {
		t.Fatalf("title=%q, want the winner's title", title)
	}
}

// TestCompleteTaskIdempotentRepeat — повтор завершения с тем же ключом
// и тем же task_id дословно возвращает сохранённый ответ и не делает
// второй мутации: версия задачи и completed_at не меняются, в журнале
// одна запись с ответом.
func TestCompleteTaskIdempotentRepeat(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)
	service := NewService(store)

	taskID := uuid.MustParse("a4900000-0000-0000-0000-000000000001")
	seedTask(t, store, taskID, "to be completed once")
	key := "idem-" + uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req := CompleteTaskRequest{TenantID: alphaTenant, TaskID: taskID, IdempotencyKey: key}
	first, err := service.CompleteTask(ctx, req)
	if err != nil {
		t.Fatalf("first complete failed: %v", err)
	}
	if first.Status != "done" {
		t.Fatalf("first response status=%q, want done", first.Status)
	}

	repeat, err := service.CompleteTask(ctx, req)
	if err != nil {
		t.Fatalf("repeat complete failed: %v", err)
	}
	if !reflect.DeepEqual(first, repeat) {
		t.Fatalf("repeat response %#v differs from first %#v", repeat, first)
	}

	err = store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		var status string
		var version int64
		var ledgerCount, withResponse int
		if err := tx.QueryRow(ctx,
			"SELECT status, version FROM tasks WHERE id=$1", taskID,
		).Scan(&status, &version); err != nil {
			return err
		}
		if status != "done" || version != 2 {
			t.Fatalf("task status=%q version=%d, want done 2 (exactly one mutation)", status, version)
		}
		if err := tx.QueryRow(ctx,
			"SELECT count(*), count(response_body) FROM command_requests WHERE key=$1", key,
		).Scan(&ledgerCount, &withResponse); err != nil {
			return err
		}
		if ledgerCount != 1 || withResponse != 1 {
			t.Fatalf("ledger=%d withResponse=%d, want 1 and 1", ledgerCount, withResponse)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCompleteTaskSameKeyDifferentTask — тот же ключ идемпотентности с
// другим task_id — ошибка клиента ErrIdempotencyConflict, при этом
// состояние обеих задач остаётся корректным: первая done, вторая open.
func TestCompleteTaskSameKeyDifferentTask(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)
	service := NewService(store)

	taskA := uuid.MustParse("a5900000-0000-0000-0000-000000000001")
	taskB := uuid.MustParse("a5900000-0000-0000-0000-000000000002")
	seedTask(t, store, taskA, "first target")
	seedTask(t, store, taskB, "second target")
	key := "idem-" + uuid.NewString()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := service.CompleteTask(ctx, CompleteTaskRequest{
		TenantID: alphaTenant, TaskID: taskA, IdempotencyKey: key,
	}); err != nil {
		t.Fatalf("first complete failed: %v", err)
	}

	_, err := service.CompleteTask(ctx, CompleteTaskRequest{
		TenantID: alphaTenant, TaskID: taskB, IdempotencyKey: key,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("got %v, want ErrIdempotencyConflict", err)
	}

	err = store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		var statusA, statusB string
		var versionA, versionB int64
		if err := tx.QueryRow(ctx,
			"SELECT status, version FROM tasks WHERE id=$1", taskA,
		).Scan(&statusA, &versionA); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx,
			"SELECT status, version FROM tasks WHERE id=$1", taskB,
		).Scan(&statusB, &versionB); err != nil {
			return err
		}
		if statusA != "done" || versionA != 2 {
			t.Fatalf("task A status=%q version=%d, want done 2", statusA, versionA)
		}
		if statusB != "open" || versionB != 1 {
			t.Fatalf("task B status=%q version=%d, want open 1 (untouched)", statusB, versionB)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSerializableKeepsTenantIsolation — фиксация того, что переход на
// SERIALIZABLE не ослабляет изоляцию арендаторов: в сериализуемой
// транзакции Alpha чужие строки Beta по-прежнему невидимы (USING), а
// попытка вставить строку Beta отклоняется политикой (WITH CHECK,
// SQLSTATE 42501).
func TestSerializableKeepsTenantIsolation(t *testing.T) {
	store, _ := testStore(t)
	betaTenant := uuid.MustParse("20000000-0000-0000-0000-000000000002")
	betaProject := uuid.MustParse("22000000-0000-0000-0000-000000000002")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Чтение: в SERIALIZABLE-транзакции Alpha строки Beta скрыты RLS.
	err := store.WithTenantTxOptions(ctx, alphaTenant, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
		var betaVisible int
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM tasks WHERE tenant_id=$1", betaTenant,
		).Scan(&betaVisible); err != nil {
			return err
		}
		if betaVisible != 0 {
			t.Fatalf("beta tasks visible to alpha serializable tx: %d", betaVisible)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Запись: вставка строки арендатора Beta из транзакции Alpha
	// отклоняется WITH CHECK политики.
	err = store.WithTenantTxOptions(ctx, alphaTenant, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,$4)`,
			betaTenant, uuid.New(), betaProject, "rls probe")
		return err
	})
	if err == nil {
		t.Fatal("cross-tenant insert in serializable tx unexpectedly succeeded")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("cross-tenant insert error = %v, want SQLSTATE 42501 (RLS)", err)
	}
}
