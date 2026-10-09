// Вариант А (РХД-03, раздел 4): оптимистическое редактирование задачи.
//
// Механизм — условный атомарный UPDATE с проверкой версии: клиент читает
// version, показывает данные пользователю (форма может быть открыта
// минутами), а запись выполняется одним оператором, где проверка версии
// и инкремент неразрывны (сервер блокирует изменяемую строку на время
// транзакции). Пустой результат означает: данные под пользователем
// изменились, его правку принимать нельзя.
package command

import (
	"errors"

	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrVersionConflict — задача изменилась (или исчезла) с момента чтения:
// UPDATE с expectedVersion затронул 0 строк. Отдельного различения
// «не найдено» сознательно нет: при конкурентном редактировании
// дополнительный SELECT для различения сам устарел бы к моменту ответа,
// а вызывающему в обоих случаях нужно одно действие — перечитать данные
// и повторить правку. Проверяется через errors.Is.
var ErrVersionConflict = errors.New("task version conflict")

// UpdateTaskTitle меняет заголовок задачи одним условным оператором.
// Уровень READ COMMITTED достаточен: атомарность однострочного UPDATE
// («проверил version — инкрементировал») сервер обеспечивает сам, а
// правила над наборами строк эта команда не проверяет. Транзакция
// арендаря (WithTenantTx) сохраняет RLS-изоляцию.
func (s *Service) UpdateTaskTitle(
	ctx context.Context,
	tenantID uuid.UUID,
	taskID uuid.UUID,
	expectedVersion int64,
	newTitle string,
) (int64, error) {
	var newVersion int64
	err := s.store.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
UPDATE tasks
SET title=$1, version=version+1, updated_at=clock_timestamp()
WHERE tenant_id=$2 AND id=$3 AND version=$4
RETURNING version`,
			newTitle, tenantID, taskID, expectedVersion,
		).Scan(&newVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVersionConflict
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return newVersion, nil
}
