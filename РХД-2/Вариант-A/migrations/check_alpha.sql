-- Шаг 3 обязательной части: контекст арендатора, локальный для транзакции.
-- Адаптация под датасет варианта А работы 1: у Alpha 10 проектов x 10 000 задач,
-- поэтому внутри транзакции ожидается 100 000 строк (в базовой методичке — 10 000).
BEGIN;
SELECT set_config(
    'app.tenant_id',
    '10000000-0000-0000-0000-000000000001',
    true
);


SELECT tenant_id, count(*)
FROM tasks
GROUP BY tenant_id;


SELECT count(*) AS explicit_beta_rows
FROM tasks
WHERE tenant_id = '20000000-0000-0000-0000-000000000002';
COMMIT;


SELECT app.current_tenant_id() AS context_after_commit;
SELECT count(*) AS rows_after_commit FROM tasks;
