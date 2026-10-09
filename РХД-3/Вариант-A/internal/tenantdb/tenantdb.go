// Package tenantdb привязывает одного проверенного арендатора к одной
// транзакции базы данных. Единица работы — транзакция, а не соединение:
// настраиваемый параметр app.tenant_id устанавливается локально для
// транзакции (set_config с is_local=true) и исчезает после COMMIT/ROLLBACK,
// поэтому физическое соединение, возвращённое в пул, не наследует контекст.
package tenantdb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// WithTenantTx binds one verified tenant to one database transaction.
// The callback must not retain tx or use it after returning.
func (s *Store) WithTenantTx(
	ctx context.Context,
	tenantID uuid.UUID,
	fn func(pgx.Tx) error,
) error {
	return s.WithTenantTxOptions(ctx, tenantID, pgx.TxOptions{}, fn)
}

// WithTenantTxOptions — обобщённая форма WithTenantTx: позволяет вызывающему
// коду выбрать параметры транзакции, прежде всего уровень изоляции
// (pgx.Serializable для команд с правилами над набором строк, РХД-03).
// Гарантии прежние: установка app.tenant_id и вся работа обработчика
// происходят в одной транзакции на одном физическом соединении, поэтому
// политика RLS видит нужное значение при любом уровне изоляции.
func (s *Store) WithTenantTxOptions(
	ctx context.Context,
	tenantID uuid.UUID,
	options pgx.TxOptions,
	fn func(pgx.Tx) error,
) (err error) {
	tx, err := s.pool.BeginTx(ctx, options)
	if err != nil {
		return fmt.Errorf("begin tenant transaction: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), time.Second,
		)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	_, err = tx.Exec(ctx,
		"SELECT set_config('app.tenant_id', $1, true)",
		tenantID.String(),
	)
	if err != nil {
		return fmt.Errorf("set tenant context: %w", err)
	}

	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tenant transaction: %w", err)
	}
	return nil
}
