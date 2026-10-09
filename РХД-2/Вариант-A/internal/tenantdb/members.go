// Методы репозитория участников проекта (вариант А, РХД-02).
//
// Сознательное решение: методы размещены в том же пакете tenantdb, а не в
// отдельном internal/members. Причина: ListProjectMembers обязан выполняться
// исключительно через ту же обёртку WithTenantTx над тем же пулом; отдельный
// пакет либо дублировал бы Store, либо открывал второй путь к базе мимо
// границы «одна транзакция — один арендатор». Единая точка входа в tenantdb
// упрощает аудит: любой код, работающий с данными арендатора, проходит через
// одну сигнатуру с uuid.UUID-аргументом.
package tenantdb

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Member — строка таблицы project_members.
type Member struct {
	TenantID  uuid.UUID
	ProjectID uuid.UUID
	UserID    uuid.UUID
	Role      string
	CreatedAt time.Time
}

// ListProjectMembers возвращает участников одного проекта одного арендатора.
// Чтение выполняется исключительно внутри WithTenantTx: в транзакции с уже
// установленным контекстом app.tenant_id, поэтому даже при ошибке в тексте
// запроса правило RLS project_members_isolation скрывает чужие строки.
func (s *Store) ListProjectMembers(
	ctx context.Context,
	tenantID uuid.UUID,
	projectID uuid.UUID,
) ([]Member, error) {
	var members []Member
	err := s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
SELECT tenant_id, project_id, user_id, role, created_at
FROM project_members
WHERE tenant_id = $1 AND project_id = $2
ORDER BY user_id`,
			tenantID, projectID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var m Member
			if err := rows.Scan(&m.TenantID, &m.ProjectID, &m.UserID, &m.Role, &m.CreatedAt); err != nil {
				return err
			}
			members = append(members, m)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return members, nil
}
