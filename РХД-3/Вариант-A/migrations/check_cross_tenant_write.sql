-- Шаг 4 обязательной части: попытка записи в чужого арендатора.
-- INSERT с tenant_id=Beta из транзакции с контекстом Alpha.
-- Ожидаемый результат: ERROR: new row violates row-level security policy + ROLLBACK.
-- Запускается БЕЗ ON_ERROR_STOP: ошибка здесь — ожидаемая часть эксперимента.
BEGIN;
SELECT set_config(
    'app.tenant_id',
    '10000000-0000-0000-0000-000000000001',
    true
);


INSERT INTO tasks (tenant_id, id, project_id, title)
VALUES (
    '20000000-0000-0000-0000-000000000002',
    'b9999999-0000-0000-0000-000000000002',
    '22000000-0000-0000-0000-000000000002',
    'must be rejected by RLS'
);
COMMIT;
