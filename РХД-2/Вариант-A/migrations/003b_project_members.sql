-- Вариант А (РХД-02, раздел 4): участники проекта.
-- Таблица project_members с составным внешним ключом (tenant_id, project_id),
-- который не допускает ссылку на проект другого арендатора, RLS ENABLE+FORCE
-- и политикой изоляции по самой строке. Сид: участники первого проекта Alpha
-- (owner/editor/viewer, детерминированные uuid) и пара участников проекта Beta.
BEGIN;


CREATE TABLE project_members (
    tenant_id  uuid NOT NULL,
    project_id uuid NOT NULL,
    user_id    uuid NOT NULL,
    role       text NOT NULL CHECK (role IN ('owner', 'editor', 'viewer')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id, user_id),
    -- Составной FK: пара (tenant_id, project_id) обязана существовать в projects,
    -- поэтому строка «участник арендатора Alpha в проекте Beta» невозможна
    -- на уровне ссылочной целостности, независимо от RLS.
    FOREIGN KEY (tenant_id, project_id)
        REFERENCES projects (tenant_id, id)
);


ALTER TABLE project_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_members FORCE ROW LEVEL SECURITY;


CREATE POLICY project_members_isolation ON project_members
    FOR ALL TO teamboard_app
    USING (tenant_id = app.current_tenant_id())
    WITH CHECK (tenant_id = app.current_tenant_id());


GRANT SELECT, INSERT, UPDATE, DELETE
ON project_members
TO teamboard_app;


-- Сид: Alpha Core — owner, editor, viewer.
INSERT INTO project_members (tenant_id, project_id, user_id, role) VALUES
('10000000-0000-0000-0000-000000000001',
 '11000000-0000-0000-0000-000000000001',
 '10000000-0000-0000-0000-000000000101', 'owner'),
('10000000-0000-0000-0000-000000000001',
 '11000000-0000-0000-0000-000000000001',
 '10000000-0000-0000-0000-000000000102', 'editor'),
('10000000-0000-0000-0000-000000000001',
 '11000000-0000-0000-0000-000000000001',
 '10000000-0000-0000-0000-000000000103', 'viewer');


-- Сид: Beta Core — owner и viewer.
INSERT INTO project_members (tenant_id, project_id, user_id, role) VALUES
('20000000-0000-0000-0000-000000000002',
 '22000000-0000-0000-0000-000000000002',
 '20000000-0000-0000-0000-000000000201', 'owner'),
('20000000-0000-0000-0000-000000000002',
 '22000000-0000-0000-0000-000000000002',
 '20000000-0000-0000-0000-000000000202', 'viewer');


COMMIT;
