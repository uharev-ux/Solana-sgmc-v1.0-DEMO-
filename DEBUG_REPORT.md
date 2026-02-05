# Architectural E2E Self-Check (DEBUG_REPORT)

Диагностика цепочки: CLI → пайплайн → DB → анализ → prune → отчёты. Без рефакторинга логики.

## Как запустить весь прогон одной командой

Из **корня проекта** (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/debug_all.ps1
```

Или по шагам:

| Шаг | Команда |
|-----|--------|
| 1 | `python scripts/arch_check.py` |
| 2 | `powershell -ExecutionPolicy Bypass -File scripts/cli_check.ps1` |
| 3 | `python scripts/db_check.py` |
| 4 | `python scripts/smoke_test.py` |
| 5 | `powershell -ExecutionPolicy Bypass -File scripts/e2e_live.ps1` |
| 6 | `python scripts/link_check.py` |
| 7 | `powershell -ExecutionPolicy Bypass -File scripts/prune_check.ps1` |

Рекомендация: везде использовать `python -m` для CLI (как в скриптах):

- `python -m dexscreener_screener.cli -h`
- `python -m dexscreener_screener.cli trigger --once --db .\debug_live.sqlite`
- `python -m dexscreener_screener.cli collect --pairs pairs_one.txt --db .\debug_live.sqlite`
- `python -m dexscreener_screener.cli prune --dry-run --db .\debug_live.sqlite`

## Ожидаемые PASS-выводы

- **arch_check**: в конце строка `ARCH_CHECK: OK`.
- **cli_check**: `PASS: cli -h`, `PASS: trigger -h`, `trigger --once --db nonexistent.sqlite => exit 1 (expected 1)`, `CLI_CHECK: OK`.
- **db_check**: список таблиц и колонок, в конце `DB_CHECK: OK`.
- **smoke_test**: серия `OK` по шагам и в конце `All smoke tests passed.`
- **e2e_live**: вывод `--- E2E_LIVE summary ---`, счётчики по `signal_events`, `signal_trigger_evaluations`, `snapshots`, `pairs`, `tokens`, затем `E2E_LIVE: OK`.
- **link_check**: `OK: signal_event + insert_trigger_eval_pending -> PENDING row`, `OK: run_trigger_analysis -> status=DONE outcome=...`, `LINK_CHECK: OK`.
- **prune_check**: `PASS: prune --dry-run`, `PASS: prune`, `PASS: schema intact after prune`, `PRUNE_CHECK: OK`.

## Что означает FAIL по каждому шагу

| Шаг | Возможная причина FAIL |
|-----|-------------------------|
| **arch_check** | Ошибка импорта (`dexscreener_screener`, `cli`, `storage.sqlite`, `engine`, `trigger_analyzer`) или ошибка `compileall.compile_dir`. Проверить PYTHONPATH и зависимости. |
| **cli_check** | CLI не запускается (`python -m dexscreener_screener.cli`), help не выводится, или при `trigger --once --db nonexistent.sqlite` код выхода не 1. Проверить установку пакета и что несуществующая БД даёт exit 1. |
| **db_check** | Нет одной из таблиц `tokens`, `pairs`, `snapshots`, `signal_events`, `signal_trigger_evaluations` или в `signal_trigger_evaluations` нет колонок `signal_id`, `status`, `evaluated_at`. Проверить схему в `storage/sqlite.py`. |
| **smoke_test** | Падает один из smoke-тестов (API, DB, export, bootstrap, post-analysis, trigger, strategy_selfcheck, prune). Смотреть последний вывод `FAIL` или `SKIP`. |
| **e2e_live** | Нет `pairs_one.txt` (тогда создаётся пустая БД, strategy/trigger могут дать нулевые счётчики — это OK). Или ошибка при `collect`/`strategy`/`trigger`. Или падение скрипта `e2e_live_summary.py`. В демо без реального `run`/collect допустимо: шаг помечен как опциональный; при отсутствии данных E2E может быть NOT APPLICABLE (см. ниже). |
| **link_check** | После `insert_signal_event` + `insert_trigger_eval_pending` нет строки со статусом PENDING в `signal_trigger_evaluations`; или после `run_trigger_analysis` статус не DONE и не NO_DATA. Проверить связку engine → insert_signal_event → insert_trigger_eval_pending и trigger_analyzer. |
| **prune_check** | `prune --dry-run` или `prune` возвращают ненулевой код; после prune отсутствует одна из таблиц (tokens, pairs, snapshots, signal_events, signal_trigger_evaluations). Требуется существующий `debug_live.sqlite` (обычно создаётся шагом e2e_live). Шаг опциональный: если prune не используется, можно пропустить. |

## Live E2E: NOT APPLICABLE

Если в демо **нет** команды run/collect или нет файла `pairs_one.txt`:

- Шаг **e2e_live** создаёт пустую БД (только схема), запускает `strategy --once` и `trigger --once`, выводит E2E_LIVE summary с нулевыми счётчиками — это допустимо и считается проходом.
- В отчёте можно пометить: *Live E2E (collect): NOT APPLICABLE — нет входных пар; проверена только схема и вызов trigger.*

Полная проверка цепочки «collect → strategy → trigger» делается при наличии `pairs_one.txt` и сети (collect ходит в API).

## Файлы

- `scripts/arch_check.py` — импорты и compileall.
- `scripts/cli_check.ps1` — help и коды возврата CLI.
- `scripts/db_check.py` — временная БД, схема, таблицы и колонки.
- `scripts/e2e_live.ps1` — один цикл (collect при наличии pairs_one.txt) + strategy + trigger + SQL summary.
- `scripts/e2e_live_summary.py` — вывод счётчиков по БД (signal_events, signal_trigger_evaluations, snapshots, pairs, tokens).
- `scripts/link_check.py` — сигнал → PENDING → run_trigger_analysis → DONE/NO_DATA.
- `scripts/prune_check.ps1` — prune --dry-run, prune, проверка схемы (через `prune_schema_check.py`).
- `scripts/prune_schema_check.py` — проверка наличия таблиц после prune (вызывается из prune_check.ps1).
- `scripts/debug_all.ps1` — последовательный запуск всех шагов с PASS/FAIL.

Все команды рассчитаны на **PowerShell** (никаких bash).
