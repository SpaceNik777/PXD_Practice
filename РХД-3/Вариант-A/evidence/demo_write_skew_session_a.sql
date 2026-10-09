-- Демон перекоса записи (РХД-03, обязательная часть, шаг «демонстрация
-- аномалии»). Сессия A, уровень READ COMMITTED, роль teamboard_app.
--
-- Инвариант предметной области: задачи 'Existing one' (…0001) и
-- 'Existing two' (…0002) не могут быть завершены одновременно —
-- завершать свою разрешено, только если партнёр ещё open.
--
-- A читает статус партнёра (…0002), убеждается, что он open, затем держит
-- окно pg_sleep, чтобы сессия B успела сделать симметричное чтение, и
-- только после этого завершает свою задачу (…0001).
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT set_config('app.tenant_id', '10000000-0000-0000-0000-000000000001', true);
SELECT 'A: partner …0002 status before decision' AS step, status
FROM tasks
WHERE tenant_id = '10000000-0000-0000-0000-000000000001'
  AND id = 'a1900000-0000-0000-0000-000000000002';
SELECT pg_sleep(3) AS a_holds_window_for_b_read;
UPDATE tasks SET status = 'done', version = version + 1, updated_at = clock_timestamp()
WHERE tenant_id = '10000000-0000-0000-0000-000000000001'
  AND id = 'a1900000-0000-0000-0000-000000000001';
COMMIT;
