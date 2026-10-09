-- Шаг 1 обязательной части (РХД-03, раздел 3): журнал идемпотентных команд
-- и демонстрационный проект для опытов с конкурентными транзакциями.
--
-- Таблица command_requests хранит связку «аренддатор + ключ идемпотентности»:
-- ровно одна предметная мутация на ключ, сколько бы дубликатов запроса ни
-- пришло. CHECK фиксирует в самой базе два допустимых состояния записи:
-- «ключ занят, работа не завершена» (response_body IS NULL, completed_at
-- IS NULL) либо «работа завершена, ответ сохранён» (оба NOT NULL).
--
-- Демонстрационный проект 'Concurrency demo' (аренддатор Alpha, лимит 3,
-- две открытые задачи) изолирует опыты работ 3 от основного датасета
-- варианта А (Alpha 100 000 / Beta 10 000 задач): перекос записи и
-- идемпотентные команды воспроизводятся на двух выделенных задачах,
-- которые миграция приводит к исходному состоянию при повторном накате.
BEGIN;


INSERT INTO projects (tenant_id, id, name, open_task_limit)
VALUES (
    '10000000-0000-0000-0000-000000000001',
    '11990000-0000-0000-0000-000000000001',
    'Concurrency demo',
    3
)
ON CONFLICT (tenant_id, id) DO UPDATE
SET open_task_limit = EXCLUDED.open_task_limit;


DELETE FROM tasks
WHERE tenant_id = '10000000-0000-0000-0000-000000000001'
  AND project_id = '11990000-0000-0000-0000-000000000001';


INSERT INTO tasks (tenant_id, id, project_id, title) VALUES
('10000000-0000-0000-0000-000000000001',
 'a1900000-0000-0000-0000-000000000001',
 '11990000-0000-0000-0000-000000000001', 'Existing one'),
('10000000-0000-0000-0000-000000000001',
 'a1900000-0000-0000-0000-000000000002',
 '11990000-0000-0000-0000-000000000001', 'Existing two');


CREATE TABLE IF NOT EXISTS command_requests (
    tenant_id      uuid NOT NULL REFERENCES tenants(id),
    key            text NOT NULL CHECK (length(key) BETWEEN 8 AND 200),
    command_type   text NOT NULL,
    request_hash   text NOT NULL,
    response_body  jsonb,
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at   timestamptz,
    PRIMARY KEY (tenant_id, key),
    CHECK ((response_body IS NOT NULL) = (completed_at IS NOT NULL))
);


-- Срок хранения записей журнала не может быть короче окна повторов клиента,
-- поэтому уборка старых записей должна быть дешёвой: индекс по created_at.
CREATE INDEX IF NOT EXISTS command_requests_retention
    ON command_requests (created_at);


ALTER TABLE command_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE command_requests FORCE ROW LEVEL SECURITY;


DROP POLICY IF EXISTS command_requests_isolation ON command_requests;
CREATE POLICY command_requests_isolation ON command_requests
    FOR ALL TO teamboard_app
    USING (tenant_id = app.current_tenant_id())
    WITH CHECK (tenant_id = app.current_tenant_id());


-- Журнал содержит фрагменты запросов и ответов арендатора, поэтому
-- изолируется так же строго, как основные таблицы. Роли приложения
-- достаточно SELECT (чтение сохранённого ответа), INSERT (занятие ключа)
-- и UPDATE (запись ответа по завершении); удаление записей журнала —
-- задача обслуживания и выполняется отдельной ролью, поэтому DELETE
-- рабочей роли не выдаётся.
GRANT SELECT, INSERT, UPDATE
ON command_requests TO teamboard_app;


COMMIT;
