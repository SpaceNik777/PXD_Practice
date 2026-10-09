-- Фиксация фактического поведения журнала command_requests (РХД-03):
-- 1) повторная вставка того же (tenant_id, key) без ON CONFLICT —
--    фактический текст SQLSTATE 23505;
-- 2) повторная вставка с ON CONFLICT (tenant_id,key) DO NOTHING —
--    INSERT 0 0, ровно та ветка, по которой работает CreateTask.
\set ON_ERROR_STOP off
BEGIN;
SELECT set_config('app.tenant_id', '10000000-0000-0000-0000-000000000001', true);
INSERT INTO command_requests (tenant_id, key, command_type, request_hash)
VALUES ('10000000-0000-0000-0000-000000000001', 'demo-probe-23505', 'probe', 'deadbeef01');
-- повтор без ON CONFLICT: ожидаем 23505
INSERT INTO command_requests (tenant_id, key, command_type, request_hash)
VALUES ('10000000-0000-0000-0000-000000000001', 'demo-probe-23505', 'probe', 'deadbeef01');
ROLLBACK;

BEGIN;
SELECT set_config('app.tenant_id', '10000000-0000-0000-0000-000000000001', true);
INSERT INTO command_requests (tenant_id, key, command_type, request_hash)
VALUES ('10000000-0000-0000-0000-000000000001', 'demo-probe-onconflict', 'probe', 'deadbeef02')
ON CONFLICT (tenant_id, key) DO NOTHING;
-- повтор с ON CONFLICT: ожидаем INSERT 0 0
INSERT INTO command_requests (tenant_id, key, command_type, request_hash)
VALUES ('10000000-0000-0000-0000-000000000001', 'demo-probe-onconflict', 'probe', 'deadbeef02')
ON CONFLICT (tenant_id, key) DO NOTHING;
ROLLBACK;
