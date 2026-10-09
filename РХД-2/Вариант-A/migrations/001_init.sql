CREATE TABLE tenants (
    id          uuid PRIMARY KEY,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);


CREATE TABLE projects (
    tenant_id       uuid NOT NULL REFERENCES tenants(id),
    id              uuid NOT NULL,
    name            text NOT NULL,
    open_task_limit integer NOT NULL DEFAULT 100
                    CHECK (open_task_limit > 0),
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE tasks (
    tenant_id   uuid NOT NULL,
    id          uuid NOT NULL,
    project_id  uuid NOT NULL,
    title       text NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
    status      text NOT NULL DEFAULT 'open'
                CHECK (status IN ('open', 'done')),
    version     bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, project_id)
        REFERENCES projects(tenant_id, id)
);


CREATE INDEX tasks_open_by_project
    ON tasks (tenant_id, project_id, created_at DESC, id)
    WHERE status = 'open';
