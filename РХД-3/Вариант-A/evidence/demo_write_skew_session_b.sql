-- Демон перекоса записи (РХД-03), сессия B, READ COMMITTED, teamboard_app.
-- B стартует сразу после A: пока A держит окно pg_sleep(3), B успевает
-- прочитать статус задачи …0001 (ещё 'open' — A не зафиксировалась) и,
-- считая предусловие выполненным, завершает свою задачу …0002.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT set_config('app.tenant_id', '10000000-0000-0000-0000-000000000001', true);
SELECT 'B: partner …0001 status before decision' AS step, status
FROM tasks
WHERE tenant_id = '10000000-0000-0000-0000-000000000001'
  AND id = 'a1900000-0000-0000-0000-000000000001';
UPDATE tasks SET status = 'done', version = version + 1, updated_at = clock_timestamp()
WHERE tenant_id = '10000000-0000-0000-0000-000000000001'
  AND id = 'a1900000-0000-0000-0000-000000000002';
COMMIT;
