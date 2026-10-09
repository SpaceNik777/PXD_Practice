package tenantdb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"example.com/teamboard/internal/tenantdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	alpha = uuid.MustParse("10000000-0000-0000-0000-000000000001")
	beta  = uuid.MustParse("20000000-0000-0000-0000-000000000002")
)

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("APP_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://teamboard_app:teamboard_app@localhost:5432/teamboard?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestTenantCannotReadOtherTenant(t *testing.T) {
	store := tenantdb.New(openPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Адаптация под датасет варианта А работы 1: у Alpha
	// 10 проектов x 10 000 задач = 100 000 строк (в базовой методичке — 10 000).
	// РХД-03 добавляет миграцией 004_idempotency две демонстрационные
	// задачи проекта 'Concurrency demo' (барьерные опыты и идемпотентные
	// команды), поэтому собственный count Alpha = 100 002; тесты пакета
	// command восстанавливают demo-проект к двум задачам после каждого
	// опыта, так что число детерминировано и при общем go test ./...
	err := store.WithTenantTx(ctx, alpha, func(tx pgx.Tx) error {
		var ownCount int
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM tasks",
		).Scan(&ownCount); err != nil {
			return err
		}
		if ownCount != 100_002 {
			t.Fatalf("alpha count = %d, want 100002", ownCount)
		}

		var foreignCount int
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM tasks WHERE tenant_id=$1", beta,
		).Scan(&foreignCount); err != nil {
			return err
		}
		if foreignCount != 0 {
			t.Fatalf("visible beta rows = %d, want 0", foreignCount)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTenantCannotInsertForOtherTenant(t *testing.T) {
	store := tenantdb.New(openPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := store.WithTenantTx(ctx, alpha, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO tasks (tenant_id,id,project_id,title)
VALUES ($1,$2,$3,'cross tenant attempt')`,
			beta,
			uuid.New(),
			uuid.MustParse("22000000-0000-0000-0000-000000000002"),
		)
		return err
	})
	if err == nil {
		t.Fatal("cross-tenant insert unexpectedly succeeded")
	}
}

func TestTenantContextDoesNotLeakAfterCommit(t *testing.T) {
	pool := openPool(t)
	store := tenantdb.New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := store.WithTenantTx(ctx, alpha, func(tx pgx.Tx) error {
		var count int
		return tx.QueryRow(ctx, "SELECT count(*) FROM tasks").Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM tasks").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("tenant context leaked through pool: visible rows=%d", count)
	}
}
