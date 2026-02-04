# Solana-sgmc-v1.0-DEMO-
Страшное позади, самое страшное впереди)

---

## DexScreener Screener v1 — Quickstart

CLI для сбора и нормализации данных по парам Solana из публичного API DexScreener. Никакой торговли, только чтение API и сохранение в SQLite. Используется только публичный DexScreener API по сети; ключи и секреты не требуются.

### Требования

- Python 3.10+

### Установка

1. Создать виртуальное окружение и активировать его:

   **Windows (PowerShell):**
   ```powershell
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   ```

   **Linux / macOS:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

2. Установить зависимости:

   ```bash
   pip install -r requirements.txt
   ```

   Или установить пакет в режиме разработки:

   ```bash
   pip install -e .
   ```

### Примеры команд

**Сбор по списку токенов (mint-адреса):**

Файл `tokens.csv` — одна колонка с адресами токенов (или одна строка с адресами через запятую):

```bash
python -m dexscreener_screener.cli collect --tokens tokens.csv --db dexscreener.sqlite
```

**Сбор по списку пар (pair addresses):**

Файл `pairs.csv` — одна колонка с адресами пар:

```bash
python -m dexscreener_screener.cli collect --pairs pairs.csv --db dexscreener.sqlite
```

**Выгрузка данных из SQLite:**

JSON:

```bash
python -m dexscreener_screener.cli export --table snapshots --format json --out snapshots.json --db dexscreener.sqlite
```

CSV:

```bash
python -m dexscreener_screener.cli export --table snapshots --format csv --out snapshots.csv --db dexscreener.sqlite
```

Можно выгружать таблицы `snapshots`, `pairs` или `tokens`:

```bash
python -m dexscreener_screener.cli export --table pairs --format json --out pairs.json --db dexscreener.sqlite
```
