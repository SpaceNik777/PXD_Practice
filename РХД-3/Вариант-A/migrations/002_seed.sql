INSERT INTO tenants (id, name) VALUES
('10000000-0000-0000-0000-000000000001', 'Alpha'),
('20000000-0000-0000-0000-000000000002', 'Beta');


INSERT INTO projects (tenant_id, id, name) VALUES
('10000000-0000-0000-0000-000000000001',
 '11000000-0000-0000-0000-000000000001', 'Alpha Core'),
('20000000-0000-0000-0000-000000000002',
 '22000000-0000-0000-0000-000000000002', 'Beta Core');


INSERT INTO tasks (tenant_id, id, project_id, title, status, created_at)
SELECT
    '10000000-0000-0000-0000-000000000001'::uuid,
    ('a0000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
    '11000000-0000-0000-0000-000000000001'::uuid,
    'Alpha task ' || g,
    CASE WHEN g % 4 = 0 THEN 'done' ELSE 'open' END,
    timestamptz '2026-09-01 00:00:00+00' - make_interval(secs => 10000 - g)
FROM generate_series(1, 10000) AS g;


INSERT INTO tasks (tenant_id, id, project_id, title, status, created_at)
SELECT
    '20000000-0000-0000-0000-000000000002'::uuid,
    ('b0000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
    '22000000-0000-0000-0000-000000000002'::uuid,
    'Beta task ' || g,
    CASE WHEN g % 4 = 0 THEN 'done' ELSE 'open' END,
    timestamptz '2026-09-01 00:00:00+00' - make_interval(secs => 10000 - g)
FROM generate_series(1, 10000) AS g;


ANALYZE;
