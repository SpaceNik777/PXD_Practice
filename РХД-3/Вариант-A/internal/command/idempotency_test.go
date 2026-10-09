package command

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TestIdempotencyConcurrentDuplicate — самый неприятный случай повтора:
// не последовательный запрос через секунду, а два одновременных вызова
// по общему стартовому сигналу (клиент повторил по тайм-ауту, а первый
// запрос всё ещё выполняется). Ожидаются три независимых утверждения:
// обе стороны получили успех; ответы совпадают буквально; в данных ровно
// одна задача и ровно одна запись журнала — предметная мутация однократна.
func TestIdempotencyConcurrentDuplicate(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)
	service := NewService(store)

	req := CreateTaskRequest{
		TenantID:       alphaTenant,
		TaskID:         uuid.New(),
		ProjectID:      demoProject,
		Title:          "created exactly once as business effect",
		IdempotencyKey: "idem-" + uuid.NewString(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results := make(chan CreateTaskResponse, 2)
	errs := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := service.CreateTask(ctx, req)
			results <- response
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("duplicate request failed: %v", err)
		}
	}
	var responses []CreateTaskResponse
	for response := range results {
		responses = append(responses, response)
	}
	if !reflect.DeepEqual(responses[0], responses[1]) {
		t.Fatalf("responses differ: %#v", responses)
	}

	err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		var taskCount, ledgerCount int
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM tasks WHERE id=$1", req.TaskID,
		).Scan(&taskCount); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM command_requests WHERE key=$1", req.IdempotencyKey,
		).Scan(&ledgerCount); err != nil {
			return err
		}
		if taskCount != 1 || ledgerCount != 1 {
			t.Fatalf("tasks=%d ledger=%d, want 1 and 1", taskCount, ledgerCount)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestIdempotencyDifferentPayload — ключ применён к другому содержимому.
// Молча вернуть ответ первой операции на другое тело — значит скрыть
// ошибку клиента: он уверен, что создал задачу с новым заголовком, а в
// базе лежит старая. Все разновидности расхождения (другой заголовок,
// другой идентификатор задачи, другой проект) обязаны приводить к одной
// типизированной ошибке ErrIdempotencyConflict.
func TestIdempotencyDifferentPayload(t *testing.T) {
	store, _ := testStore(t)
	demoEnv(t, store)
	service := NewService(store)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	first := CreateTaskRequest{
		TenantID:       alphaTenant,
		TaskID:         uuid.MustParse("a2900000-0000-0000-0000-000000000001"),
		ProjectID:      demoProject,
		Title:          "original title",
		IdempotencyKey: "idem-" + uuid.NewString(),
	}
	if _, err := service.CreateTask(ctx, first); err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	cases := []struct {
		name string
		mod  func(r CreateTaskRequest) CreateTaskRequest
	}{
		{
			name: "same key, different title",
			mod: func(r CreateTaskRequest) CreateTaskRequest {
				r.Title = "changed title"
				return r
			},
		},
		{
			name: "same key, different task id",
			mod: func(r CreateTaskRequest) CreateTaskRequest {
				r.TaskID = uuid.MustParse("a2900000-0000-0000-0000-000000000002")
				return r
			},
		},
		{
			name: "same key, different project",
			mod: func(r CreateTaskRequest) CreateTaskRequest {
				r.ProjectID = uuid.MustParse("11000000-0000-0000-0000-000000000001")
				return r
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replayed := tc.mod(first)
			_, err := service.CreateTask(ctx, replayed)
			if !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("got %v, want ErrIdempotencyConflict", err)
			}
		})
	}

	// Предметные данные после отказов консистентны: первая задача на
	// месте, ничего лишнего не создано, журнал содержит одну запись.
	err := store.WithTenantTx(ctx, alphaTenant, func(tx pgx.Tx) error {
		var taskCount, ledgerCount int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM tasks WHERE id=$1",
			first.TaskID).Scan(&taskCount); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM command_requests WHERE key=$1",
			first.IdempotencyKey).Scan(&ledgerCount); err != nil {
			return err
		}
		if taskCount != 1 || ledgerCount != 1 {
			t.Fatalf("tasks=%d ledger=%d, want 1 and 1", taskCount, ledgerCount)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
