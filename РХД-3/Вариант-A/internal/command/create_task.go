package command

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"example.com/teamboard/internal/tenantdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrIdempotencyConflict — тот же ключ применён к другому содержимому
	// запроса (другая операция, другое тело). Ошибка клиента, а не
	// конкуренции: не повторяется никогда. Проверяется через errors.Is.
	ErrIdempotencyConflict = errors.New("idempotency key reused with another request")
	// ErrOpenTaskLimit — отказ по бизнес-правилу; транзакция, включая
	// занятие ключа, откатывается, поэтому ключ не «сжигается».
	ErrOpenTaskLimit = errors.New("open task limit reached")
)

// CreateTaskRequest — предметная часть запроса плюс ключ идемпотентности.
// TaskID задаётся клиентом до цикла попыток: если идентификатор
// генерировать на сервере при каждой попытке, повтор описывал бы «другую»
// задачу и отпечаток запроса перестал бы совпадать.
type CreateTaskRequest struct {
	TenantID       uuid.UUID `json:"tenant_id"`
	TaskID         uuid.UUID `json:"task_id"`
	ProjectID      uuid.UUID `json:"project_id"`
	Title          string    `json:"title"`
	IdempotencyKey string    `json:"-"`
}

type CreateTaskResponse struct {
	TaskID  uuid.UUID `json:"task_id"`
	Status  string    `json:"status"`
	Version int64     `json:"version"`
}

type Service struct {
	store *tenantdb.Store
}

func NewService(store *tenantdb.Store) *Service {
	return &Service{store: store}
}

// CreateTask — идемпотентная команда создания задачи. Ключ занимается
// первым оператором транзакции (ON CONFLICT DO NOTHING): одновременный
// дубликат встаёт в ожидание на ограничении уникальности ещё до подсчёта
// задач и не может увидеть промежуточное состояние. При RowsAffected==1
// обработчик — первый: выполняет предметную работу и записывает ответ
// вместе с completed_at одним UPDATE (CHECK в схеме не позволит записать
// одно без другого). При 0 строк — вычитывается сохранённый ответ;
// несовпадение операции или отпечатка — ErrIdempotencyConflict.
func (s *Service) CreateTask(
	ctx context.Context,
	req CreateTaskRequest,
) (CreateTaskResponse, error) {
	// Отпечаток считается один раз, до входа в транзакцию: он не зависит
	// от состояния базы и обязан быть одинаковым во всех попытках повтора.
	requestHash, err := hashCreateTask(req)
	if err != nil {
		return CreateTaskResponse{}, err
	}
	var response CreateTaskResponse

	err = RunSerializable(ctx, s.store, req.TenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
INSERT INTO command_requests (tenant_id,key,command_type,request_hash)
VALUES ($1,$2,'create_task',$3)
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
				if commandType != "create_task" || storedHash != requestHash {
					return ErrIdempotencyConflict
				}
				if raw != nil {
					if err := json.Unmarshal(raw, &response); err != nil {
						return fmt.Errorf("decode stored response: %w", err)
					}
					return nil
				}
				// Ключ занят конкурентом, который ещё не завершил работу
				// (response_body и completed_at ещё NULL). Обычно этот
				// конкурент отпускает ключ вместе с фиксацией ответа;
				// на всякий случай ждём completed_at IS NOT NULL в цикле.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(25 * time.Millisecond):
				}
			}
		}

		// Предметная работа первого обработчика: лимит открытых задач
		// проекта — правило над набором строк, поэтому SERIALIZABLE.
		var count, limit int
		err = tx.QueryRow(ctx, `
SELECT count(*), p.open_task_limit
FROM projects p
LEFT JOIN tasks t
  ON t.tenant_id=p.tenant_id AND t.project_id=p.id AND t.status='open'
WHERE p.id=$1
GROUP BY p.open_task_limit`, req.ProjectID).Scan(&count, &limit)
		if err != nil {
			return fmt.Errorf("read project capacity: %w", err)
		}
		if count >= limit {
			return ErrOpenTaskLimit
		}

		var version int64
		err = tx.QueryRow(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,$4)
RETURNING version`,
			req.TenantID, req.TaskID, req.ProjectID, req.Title).Scan(&version)
		if err != nil {
			return fmt.Errorf("insert task: %w", err)
		}

		response = CreateTaskResponse{TaskID: req.TaskID, Status: "open", Version: version}
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

// hashCreateTask кодирует существенное содержимое запроса отдельной
// структурой с фиксированным порядком полей: порядок обхода отображения
// в Go не определён, и отпечаток от него был бы нестабильным. Арендатор
// не входит в отпечаток — он и так в первичном ключе журнала; ключ тоже
// не входит — он адресует запись, а не описывает запрос.
func hashCreateTask(req CreateTaskRequest) (string, error) {
	payload := struct {
		TaskID    uuid.UUID `json:"task_id"`
		ProjectID uuid.UUID `json:"project_id"`
		Title     string    `json:"title"`
	}{req.TaskID, req.ProjectID, req.Title}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode request hash input: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
