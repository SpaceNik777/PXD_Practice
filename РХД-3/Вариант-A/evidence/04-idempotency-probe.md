# РХД-03. Фактическое поведение журнала command_requests

Проба под рабочей ролью `teamboard_app` с транзакционным
`set_config('app.tenant_id', ..., true)` (RLS включена), скрипт
`demo_idempotency_probe.sql`. Обе секции завершаются ROLLBACK, журнал
не замусорен (контроль `probe_rows_left = 0`).

## Повторная вставка того же (tenant_id, key) без ON CONFLICT

Фактический текст ошибки (SQLSTATE 23505, unique_violation):

```
ERROR:  duplicate key value violates unique constraint "command_requests_pkey"
```

## Повторная вставка с ON CONFLICT (tenant_id, key) DO NOTHING

Та самая ветка, по которой работает `CreateTask`/`CompleteTask`:

```
INSERT 0 1
INSERT 0 0
```

Число задетых строк при повторе — 0: по нему команда понимает, что она
не первый обработчик, и переходит к чтению сохранённого ответа.

## Контроль после проб

```
 probe_rows_left
-----------------
               0
(1 row)
```

Го-тесты согласуются с этими фактами: `TestIdempotencyConcurrentDuplicate`
(два одновременных вызова с одним ключом: обе стороны — успех, ответы
совпадают дословно, tasks=1, command_requests=1) и
`TestIdempotencyDifferentPayload` (другой title / другой task_id /
другой project → `ErrIdempotencyConflict` через errors.Is; в реализации
конфликт ключа гасится ON CONFLICT DO NOTHING, а несовпадение отпечатка
отклоняется явной типизированной ошибкой до какой-либо предметной работы).
