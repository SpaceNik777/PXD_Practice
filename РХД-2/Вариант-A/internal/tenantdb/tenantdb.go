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
) (err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
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
