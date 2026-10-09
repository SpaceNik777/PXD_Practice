// Package command содержит команды записи предметной области Teamboard,
// выполняемые в транзакции одного арендатора: с ограниченным повтором
// сериализуемых транзакций (retry.go) и с журналом идемпотентности
// (create_task.go, complete_task.go). РХД-03.
package command

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"example.com/teamboard/internal/tenantdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// maxTransactionAttempts — бюджет попыток: бесконечный повтор при высокой
// нагрузке создаёт положительную обратную связь «конфликты порождают
// повторы, повторы порождают конфликты», поэтому он ограничен пятью.
const maxTransactionAttempts = 5

// ErrConflictClass — маркер класса временных ошибок конкуренции
// (SQLSTATE 40001 serialization_failure и 40P01 deadlock_detected).
// Возвращается только при исчерпании бюджета попыток; сам факт ретрая
// этой ошибкой не сопровождается. Проверяется через errors.Is.
var ErrConflictClass = errors.New("transaction conflict (40001/40P01)")

// RunSerializable выполняет fn в SERIALIZABLE-транзакции арендатора и
// при ошибке класса конфликта повторяет транзакцию целиком — с нового
// BEGIN, отбрасывая все результаты неудачной попытки. fn обязано быть
// готово к многократному вызову и не совершать необратимых действий
// вне базы; значения, входящие в отпечаток идемпотентности, вычисляются
// до входа в цикл попыток.
func RunSerializable(
	ctx context.Context,
	store *tenantdb.Store,
	tenantID uuid.UUID,
	fn func(pgx.Tx) error,
) error {
	for attempt := 0; attempt < maxTransactionAttempts; attempt++ {
		err := store.WithTenantTxOptions(ctx, tenantID, pgx.TxOptions{
			IsoLevel: pgx.Serializable,
		}, fn)
		if err == nil {
			return nil
		}
		if !retryableTransactionError(err) {
			return err
		}
		if attempt == maxTransactionAttempts-1 {
			return fmt.Errorf("%w: serializable retry budget exhausted: %w",
				ErrConflictClass, err)
		}

		// Экспоненциальная база (≈10, 20, 40, 80 мс) плюс джиттер до
		// половины базы, чтобы отменённые конкуренты не просыпались
		// одновременно и не воспроизводили тот же конфликт.
		base := 10 * time.Millisecond * time.Duration(1<<attempt)
		jitter := time.Duration(rand.Int64N(int64(base/2) + 1))
		timer := time.NewTimer(base + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("unreachable transaction retry state")
}

// retryableTransactionError намеренно узко: повторяются только 40001 и
// 40P01, и только если ошибка действительно пришла от сервера. Нарушение
// уникальности, ошибка в тексте запроса, неверные входные данные и отказ
// по бизнес-правилу не повторяются никогда — автоматический повтор
// невременной ошибки превращает одну неудачу в пятикратную нагрузку и
// скрывает настоящую причину сбоя от того, кто читает журналы.
func retryableTransactionError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01"
}
