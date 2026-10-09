// Вариант А (РХД-03, раздел 4), пункт 3: идемпотентная команда
// завершения задачи. Журнал command_requests связывает
// (арендатор, ключ) с результатом: два одновременных повтора возвращают
// одинаковый ответ и приводят к однократному изменению данных.
package command

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrTaskNotOpen — предметная работа невыполнима: задача не найдена или
// уже завершена. Отказ по бизнес-правилу откатывает транзакцию вместе с
// занятием ключа, поэтому ключ не «сжигается».
var ErrTaskNotOpen = errors.New("task is not open (already done or not found)")

// CompleteTaskRequest: арендатор, задача и ключ идемпотентности.
type CompleteTaskRequest struct {
	TenantID       uuid.UUID `json:"tenant_id"`
	TaskID         uuid.UUID `json:"task_id"`
	IdempotencyKey string    `json:"-"`
}

// CompleteTaskResponse — то, что сохраняется в журнал и дословно
// возвращается при повторе.
type CompleteTaskResponse struct {
	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completed_at"`
}

// CompleteTask переводит задачу в 'done' с журналом идемпотентности.
// Порядок внутри транзакции тот же, что у CreateTask: занять ключ,
// выполнить предметную работу, записать ответ и completed_at одним
// UPDATE. Повтор с тем же ключом и тем же task_id возвращает сохранённый
// ответ; тот же ключ с другим task_id — ErrIdempotencyConflict.
func (s *Service) CompleteTask(
	ctx context.Context,
	req CompleteTaskRequest,
) (CompleteTaskResponse, error) {
	requestHash, err := hashCompleteTask(req)
	if err != nil {
		return CompleteTaskResponse{}, err
	}
	var response CompleteTaskResponse

	err = RunSerializable(ctx, s.store, req.TenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
INSERT INTO command_requests (tenant_id,key,command_type,request_hash)
VALUES ($1,$2,'complete_task',$3)
ON CONFLICT (tenant_id,key) DO NOTHING`,
			req.TenantID, req.IdempotencyKey, requestHash)
		if err != nil {
			return fmt.Errorf("claim idempotency key: %w", err)
		}

		if tag.RowsAffected() == 0 {
			for {
				var commandType, storedHash string
				var raw []byte
				err := tx.QueryRow(ctx, `
SELECT command_type, request_hash, response_body
FROM command_requests
WHERE tenant_id=$1 AND key=$2`,
					req.TenantID, req.IdempotencyKey,
				).Scan(&commandType, &storedHash, &raw)
				if err != nil {
					return fmt.Errorf("read idempotency result: %w", err)
				}
				if commandType != "complete_task" || storedHash != requestHash {
					return ErrIdempotencyConflict
				}
				if raw != nil {
					if err := json.Unmarshal(raw, &response); err != nil {
						return fmt.Errorf("decode stored response: %w", err)
					}
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(25 * time.Millisecond):
				}
			}
		}

		// Предметная работа: однострочный условный UPDATE — повторный
		// перевод в done невозможен даже без журнала (WHERE status='open').
		var completedAt time.Time
		err = tx.QueryRow(ctx, `
UPDATE tasks
SET status='done', version=version+1, updated_at=clock_timestamp()
WHERE tenant_id=$1 AND id=$2 AND status='open'
RETURNING updated_at`,
			req.TenantID, req.TaskID,
		).Scan(&completedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotOpen
		}
		if err != nil {
			return fmt.Errorf("complete task: %w", err)
		}

		response = CompleteTaskResponse{Status: "done", CompletedAt: completedAt}
		raw, err := json.Marshal(response)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
UPDATE command_requests
SET response_body=$3::jsonb, completed_at=clock_timestamp()
WHERE tenant_id=$1 AND key=$2`,
			req.TenantID, req.IdempotencyKey, raw)
		return err
	})
	return response, err
}

// hashCompleteTask кодирует существенное содержимое: идентификатор
// завершаемой задачи. Ключ и арендатор в отпечаток не входят по тем же
// причинам, что и в create_task.
func hashCompleteTask(req CompleteTaskRequest) (string, error) {
	payload := struct {
		TaskID uuid.UUID `json:"task_id"`
	}{req.TaskID}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode request hash input: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
