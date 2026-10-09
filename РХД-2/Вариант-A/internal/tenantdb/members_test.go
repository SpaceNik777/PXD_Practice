// Тесты варианта А (РХД-02): участники проекта.
// Позитивный тест проверяет «ожидаемое количество своих», отрицательные —
// «ноль чужих», «невозможность кросс-записи» и «отсутствие утечки контекста».
package tenantdb_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"example.com/teamboard/internal/tenantdb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	alphaProject = uuid.MustParse("11000000-0000-0000-0000-000000000001")
	betaProject  = uuid.MustParse("22000000-0000-0000-0000-000000000002")
)

// Позитив: Alpha видит участников СВОЕГО проекта — ровно 3 строки сид-данных
// (owner, editor, viewer) и в предсказуемом порядке.
func TestListProjectMembersOwnTenant(t *testing.T) {
	store := tenantdb.New(openPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	members, err := store.ListProjectMembers(ctx, alpha, alphaProject)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Fatalf("alpha members = %d, want 3", len(members))
	}

	got := make([]string, 0, len(members))
	for _, m := range members {
		got = append(got, m.Role)
	}
	want := []string{"owner", "editor", "viewer"} // порядок по user_id: 101, 102, 103
	if !slices.Equal(got, want) {
		t.Fatalf("roles = %v, want %v", got, want)
	}
	for _, m := range members {
		if m.TenantID != alpha || m.ProjectID != alphaProject {
			t.Fatalf("foreign row leaked: %+v", m)
		}
	}
}

// Отрицательный: участники проекта Beta для Alpha — 0 строк и НЕ ошибка.
func TestAlphaCannotSeeBetaProjectMembers(t *testing.T) {
	store := tenantdb.New(openPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	members, err := store.ListProjectMembers(ctx, alpha, betaProject)
	if err != nil {
		t.Fatalf("query itself must succeed, got error: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("alpha sees beta members: %d rows, want 0", len(members))
	}
}

// Отрицательный: Alpha не может добавить участника в проект Beta.
// Вставляется строка с tenant_id=Alpha и project_id=Beta-проекта:
// RLS WITH CHECK её пропускает (tenant_id совпадает с контекстом Alpha),
// а составной FK (tenant_id, project_id) отклоняет — пары (Alpha, Beta-проект)
// нет в projects. Фиксируем фактический текст ошибки.
func TestTenantCannotAddMemberToForeignProject(t *testing.T) {
	store := tenantdb.New(openPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := store.WithTenantTx(ctx, alpha, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO project_members (tenant_id, project_id, user_id, role)
VALUES ($1, $2, $3, 'editor')`,
			alpha,
			betaProject,
			uuid.MustParse("10000000-0000-0000-0000-000000000199"),
		)
		return err
	})
	if err == nil {
		t.Fatal("cross-project member insert unexpectedly succeeded")
	}
	t.Logf("actual error: %v", err)
}

// Отрицательный: после фиксации транзакции ListProjectMembers контекст пуст —
// прямой запрос к пулу (MaxConns=1, то же физическое соединение) видит 0 строк.
func TestMembersContextDoesNotLeakAfterCommit(t *testing.T) {
	pool := openPool(t)
	store := tenantdb.New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := store.ListProjectMembers(ctx, alpha, alphaProject); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM project_members").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("tenant context leaked through pool: visible member rows=%d", count)
	}
}
