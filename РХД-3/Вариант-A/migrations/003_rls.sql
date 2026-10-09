-- Шаг 1 обязательной части (РХД-02, раздел 3):
-- рабочая роль, схема app, функция текущего арендатора и правила RLS.
BEGIN;


DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'teamboard_app') THEN
        CREATE ROLE teamboard_app LOGIN PASSWORD 'teamboard_app'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    END IF;
END
$roles$;


CREATE SCHEMA IF NOT EXISTS app;


CREATE OR REPLACE FUNCTION app.current_tenant_id()
RETURNS uuid
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $function$
    SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid
$function$;


REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public, app TO teamboard_app;
GRANT EXECUTE ON FUNCTION app.current_tenant_id() TO teamboard_app;


GRANT SELECT, INSERT, UPDATE, DELETE
ON tenants, projects, tasks
TO teamboard_app;


ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;


ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;
ALTER TABLE tasks FORCE ROW LEVEL SECURITY;


CREATE POLICY tenants_isolation ON tenants
    FOR ALL TO teamboard_app
    USING (id = app.current_tenant_id())
    WITH CHECK (id = app.current_tenant_id());


CREATE POLICY projects_isolation ON projects
    FOR ALL TO teamboard_app
    USING (tenant_id = app.current_tenant_id())
    WITH CHECK (tenant_id = app.current_tenant_id());


CREATE POLICY tasks_isolation ON tasks
    FOR ALL TO teamboard_app
    USING (tenant_id = app.current_tenant_id())
    WITH CHECK (tenant_id = app.current_tenant_id());


COMMIT;
