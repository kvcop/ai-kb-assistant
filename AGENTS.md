# Repository Guidelines (AI KB Assistant template)

**Updated**: 2026-02-02  
**Goal**: KB-first daily workflow with minimal manual edits (2 files per day for work); integrations are optional and disabled by default.

## Project Structure & Module Organization

- `scripts/kb.py` is the primary entrypoint for maintenance and daily briefing.
- `configs/kb.toml` stores the default Jira JQL (helper to fetch assigned issues); Jira is optional.
- `notes/work/` is the **active** surface (generated + lightly edited):
  - `notes/work/daily-brief.md` — morning brief (generated)
  - `notes/work/end-of-day.md` — end-of-day delta + 3–7 bullets (generated + manual notes)
- `notes/work/jira.md` — canonical JQL (optional).
- `notes/work/open-questions.md` — deferred questions to batch-resolve later (optional).
- `notes/work/todos.md` — TODO backlog; checkboxes.
- `notes/work/reminders.md` — reminders (recurring + date-based).
- `notes/meetings/` and `notes/technical/` store longer notes; link them from `notes/work/end-of-day.md`.
- `notes/personal/` stores **personal** initiatives and is not part of the default work daily flow.
- `notes/ideas/` stores raw ideas/drafts.
- `notes/daily-logs/` stores historical end-of-day copies (`YYYY-MM-DD.md`).
- `tg_bot/` is the Telegram ↔ Codex bridge (optional; the KB can be used without Telegram).
- `tg_uploads/` stores downloaded attachments (ignored by git).
- `logs/` is runtime output (ignored by git); don’t put long-term knowledge there.
- `templates/` holds reusable templates (keep them small and stable).
- `orchestrator/` (optional) contains a reproducible “staged” workflow for changes (if you use it).

## AI Assistant Process

### Defaults
- Prefer Russian unless the user asks otherwise.
- Treat the knowledge base (this repo) as the source of truth for daily state (plan/notes/time tracking).
- Keep optional integrations **off by default**. Do not assume the user has Jira / speech2text / Mattermost / local Telegram Bot API / Codex CLI installed.
- Jira is auxiliary and optional: use it to fetch “assigned issues” and to detect drift **when configured**; otherwise skip.
- Keep the active knowledge base small: prefer **generated state** over manual bookkeeping.
- Keep work vs personal separate: by default, only `notes/work/**` participates in briefings/time tracking; use `notes/personal/**` only when the user explicitly says it’s personal/weekend/pet-project context.
- User-facing onboarding/UX details (setup, `.env` examples, Telegram Topics, `/reminders`) live in `README.md`; read it when you need operational context beyond these agent rules.
- `.env` loading: do **not** use `set -a; source .env ...`; `scripts/kb.py` auto-loads `JIRA_*` from `.env` (best-effort, without overriding existing env). If Jira auth is missing/unavailable, tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`).
- Assume the user may not have quick filesystem access; for briefings, proactively read the relevant KB files and present the needed context in chat.
- When processing each new user message (especially for time tracking), first check the current local date/time (e.g., `date '+%Y-%m-%d %H:%M (%A) %Z'`) to anchor timestamps.
- Track user effort during the day: update `notes/work/end-of-day.md` → “Time Tracking (draft)” from user notes (time hints + Jira keys). Never invent hours; use `__h` placeholders and confirm at end-day.
- Time tracking line format: prefer `KEY — Название — 1:30 (что сделано; 11:00–12:30)` (or `KEY — Название — __h`).
- Транскрипции/протоколы: поддерживать базу опечаток и терминов в `notes/work/typos.md` (grep-friendly), обновлять через `python3 scripts/kb.py typos`.
- Напоминания: держать в `notes/work/reminders.md`; `python3 scripts/kb.py reminders` показывает напоминания на дату.
- Time write-off rounding: round totals **to 0.5h (30 min)** when finalizing (e.g., 22 min → 0.5h); keep exact intervals in parentheses for auditability.
- Carryover rule: bullets in `notes/work/end-of-day.md` → “Notes” that start with `TODO` or an unchecked checkbox (`- [ ] ...`) should be carried into `notes/work/open-questions.md` with an explicit target date (avoid relative labels like “tomorrow”).
- Missing-info UX: если что-то неясно / заблокировано — спроси сразу; если пользователь хочет ответить позже — добавь в `notes/work/open-questions.md` (Active). Когда пользователь говорит “могу ответить на вопросы”, батч-резолв через `python3 scripts/kb.py questions`.
- Project TODOs: maintain `notes/work/todos.md` by projects; when the user sends notes/ideas, add actionable items there with enough context + links (meeting/protocol notes).
- Вопросы пользователю (Telegram UX): использовать формат `**Вопрос №…:** / **Критичность:** / **Варианты ответа:** / **Рекомендация:**`, но **без markdown-таблиц** (в Telegram они не рендерятся) — варианты перечислять строками `A) …`, рекомендованный помечать `✓`.
- Если вопросов >10 и общение идёт через Telegram — писать их в `QUESTIONS.md` (в корне репозитория) и отправлять файлом вместо спама в чат.
- `QUESTIONS.md` не коммитить; после получения ответов — **сразу удалять** `QUESTIONS.md`.
- Не задавать вопросы про интервалы, которые ещё не могли наступить/закончиться (например “после обеда” до окончания обеда).

### Вопросы пользователю: формат по каналу

Если в контексте запроса есть признаки Telegram (например блок `TELEGRAM_BOT_CONTEXT`, поля `chat_id`/`message_thread_id`, или секция `Telegram (MCP)`), форматируй вопросы “по‑телеграмному” (без markdown-таблиц). Если запроса из Telegram нет (обычный терминал/консоль), можно использовать markdown-таблицы — они там читабельны.

**Формат для терминала/консоли (таблица):**

```
**Вопрос №1:**
    Какой формат логирования выбрать для нового сервиса?
**Критичность:**
    3. WARNING
**Варианты ответа:**
    | id | описание | рекомендовано |
    |---|---|---|
    | A | JSON | ✓ |
    | B | Plain text |  |
    | C | Syslog |  |
    | D | предложи свое коротко |  |
**Рекомендация:**
    JSON проще парсится и пригоден для централизованных систем логирования.
```

**Формат для Telegram (без таблиц):**

```
**Вопрос №1:**
    Какой формат логирования выбрать для нового сервиса?
**Критичность:**
    3. WARNING
**Варианты ответа:**
    A) JSON ✓
    B) Plain text
    C) Syslog
    D) предложи свое коротко
**Рекомендация:**
    JSON проще парсится и пригоден для централизованных систем логирования.
```

Рекомендации для Telegram: минимум отступов, лучше лишняя пустая строка между блоками, чем “широкие” таблицы.

### Briefing UX (user preference)
- When the user asks for a briefing (e.g., `day-start`, “бриф”, “план на день”): be more detailed; 2–3 screens is acceptable if it reduces back-and-forth.
- Task listing rules:
  - Expand **all High priority** issues (key, title, status) and highlight which ones are most actionable today.
  - For Normal/Low: short list is enough (key + title + status).
  - Sort/weight by status: `In progress` / `Review` first (higher priority than `To Do` / `Backlog`).
- No bare IDs: when mentioning a Jira task in chat, always include the human title next to the key (e.g., `PROJ-123 — <title>`). If the title is missing, try to resolve it from `notes/work/daily-brief.md`, the latest snapshot, or `python3 scripts/kb.py jira-issue <key|id>`; otherwise mark it as `<название?>` and (optionally) ask one clarifying question.
- Continuity: keep enough KB signals so that next-day brief can clearly show what “горит” and what “застоялось” (use yesterday EOD notes + open questions + Jira snapshot diffs; do not invent facts—ask or mark as TBD).

### Daily flow
- Start of day: `python3 scripts/kb.py day-start` → refresh `notes/work/daily-brief.md`.
- End of day: `python3 scripts/kb.py end-day` → refresh `notes/work/end-of-day.md` and write `notes/daily-logs/YYYY-MM-DD.md`, then add 3–7 short bullets in “Notes”.
- Time write-off: use `python3 scripts/kb.py time-report --jira` (day) or `python3 scripts/kb.py time-report --week --jira` (week-to-date) to generate a worklog-friendly summary for the user to confirm. Jira is optional: when not configured, the report should still work (without Jira titles).
- Persistence (TG bot / long-running service): if important KB state was updated (new notes, reminders, open questions), commit early; push if a remote is configured. Don’t commit secrets.
- Before restarting the bot/service, check `/queue` and warn the user: restart interrupts active tasks.
- Telegram files: to return artifacts back to Telegram, use `/upload <path> [--zip]` (path must be inside the repo workspace or `tg_uploads/`; size limit via `TG_SEND_MAX_MB`).
- Free time / questions: use `python3 scripts/kb.py questions` to show pending confirmations (time buckets `__h`, needs-review items, Jira hygiene, manual open questions, and project TODOs).
  - Time report formatting preference: blank line between days; each entry as `` `KEY` : `title` : `1h 30m` `` (or `` `1h` `` / `` `45m` ``) + description on the next line (also in backticks for easy copy).
  - If a Jira key has multiple distinct topics in a day: prefer multiple separate time entries so the report can be copied as several worklog items.

### Optional features (disabled by default)

Before suggesting an optional feature, verify it’s really disabled/unavailable (env flags missing, token missing, dependency not installed, service not running), then propose the smallest safe enablement.

- Jira sync (optional)
  - Enable: set `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`) in `.env`.
  - Where: `scripts/kb.py`, `configs/kb.toml`, `notes/work/jira.md`.
  - Verify: `python3 scripts/kb.py doctor`, `python3 scripts/kb.py day-start` (should not show “skipped”).
- Voice auto-transcribe via Speech2Text (optional)
  - Default: OFF.
  - Enable: set `SPEECH2TEXT_BASE_URL`, provide token (`SPEECH2TEXT_TOKEN` or `~/.config/speech2text/token`), set `TG_VOICE_AUTO_TRANSCRIBE=1`.
  - Where: `scripts/speech2text.py`, `scripts/mcp_speech2text.py`, `tg_bot/app.py` (voice handling), `tg_bot/router.py` (doctor checks).
- Local Telegram Bot API server (optional)
  - Default: OFF (`TG_BOT_API_PREFER_LOCAL=0`).
  - Enable: run a local Bot API server, set `TG_BOT_API_LOCAL_URL`, set `TG_BOT_API_PREFER_LOCAL=1`.
  - Where: `tg_bot/telegram_api.py`, `tg_bot/examples/telegram-bot-api.service`.
- Mattermost watcher (optional)
  - Default: OFF (`MM_ENABLED=0`).
  - Enable: install `mattermostdriver`, set `MM_URL` + credentials, set `MM_ENABLED=1`.
  - Where: `tg_bot/mattermost_watch.py`.
- tmux helpers (optional)
  - Where: `scripts/tmux_tools.py`, `tg_bot/tmux_tools.py`.
- MCP servers (optional)
  - Telegram send: `scripts/mcp_telegram_send.py`
  - Telegram follow-ups: `scripts/mcp_telegram_followups.py`
  - Speech2Text: `scripts/mcp_speech2text.py`

### Self-improvement loop (safe)
- Capture friction in `notes/work/end-of-day.md` under “Friction / Improvements”.
- When improving the repo:
  - prefer “report-only” analysis first (e.g., `python3 scripts/kb.py doctor`);
  - propose small diffs with a clear expected benefit;
  - never change formats/automation in a way that requires extra daily manual work.

## Build, Test, and Development Commands

- Python: use `python3` (the scripts assume Python 3.11+ for built-in TOML support).
- Prefer `uv` for venv/deps when available (`uv venv .venv`).
- If `ruff`/`mypy` are configured in `pyproject.toml` and you change Python code, run:
  - `uv run ruff format`
  - `uv run ruff check`
  - `uv run mypy`
- Smoke checks:
  - `python3 -m py_compile scripts/kb.py`
  - `python3 scripts/kb.py doctor`
- Tests:
  - `TMPDIR=$PWD/.tmp/pytests uv run python -m unittest discover -s tg_bot/tests`

## Coding Style & Naming Conventions

- Python: PEP 8, type hints, explicit `argparse`.
- Prefer standard library only for repo tooling (avoid adding dependencies unless there is a clear payoff and installation story).

## Commit & Pull Request Guidelines

- Conventional Commits (`feat:`, `fix:`, `docs:`) if/when commits are made.
- Keep changes focused; do not mix archival moves with unrelated refactors.

---

## Общие инструкции (из `~/.codex/AGENTS.md`, с минимальными правками под Markdown/Telegram)

<!-- BEGIN: copied from /home/user/.codex/AGENTS.md (minimal edits for Markdown bold) -->
# Правила работы и общения

## 0. Приоритеты требований

- MUST — обязательно.
- SHOULD — выполнять по умолчанию, если нет явной причины отклониться.
- MAY — опционально.

## 1. Общение

### 1.1 Язык (MUST)

- MUST: общаться с пользователем на русском языке.

### 1.3 Вопросы и недопонимания

#### Когда задавать вопросы

- MUST: задавать вопрос, если без ответа есть риск сделать неверную работу, затронуть API/данные, выполнить необратимые действия или потратить время/деньги не туда.
- SHOULD: задавать, если есть 2+ разумных варианта и выбор влияет на итог.
- MUST NOT: не задавать, если ответ уже обсуждался или очевидно выводится из контекста.
- MUST NOT: не задавать, если есть единственный очевидный вариант (например, один remote и текущая tracking-ветка).
- MUST NOT: не задавать вопросы вида «подтверждаешь, что принимаем рекомендованные варианты… чтобы я продолжил?». Такие вопросы бессмысленны. Если пользователь продолжил общаться на другие темы, поддерживать диалог, а не возвращать постоянно к вопросам.
- MUST: если в сообщении пользователя есть `noquestions`, автоматически принимать рекомендованный вариант на все возникающие вопросы и не задавать их (пользователь в неинтерактивном режиме).

#### Формат вопроса (MUST)

```
**Вопрос №1:**
    Что будем делать с ... / Как думаешь ... / В общем и целом - текст, отражающий *суть* проблемы, обычно в форме вопроса, но можно и обычным предложением.
**Критичность:**
    Одно из: `1. CRITICAL`, `2. ERROR`, `3. WARNING`, `4. INFO`, `5. DEBUG`
**Варианты ответа:**
    MD таблица с вариантами ответов A-D. Столбцы: id, описание, рекомендовано
**Рекомендация:**
    Тут рекомендованный вариант ответа с более длинным объяснением, чем в md-таблице.
```

#### Ограничения вопросов (MUST)

- MUST: максимум 5 вопросов за один раунд.
- MUST: максимум 4 варианта ответа; последний всегда — "предложи свое коротко".
- MUST: вопросы задаются в порядке критичности (CRITICAL → DEBUG).
- MUST: в столбце "рекомендовано" ставится ✓ напротив одного варианта.

#### Блокирующие вопросы

- MUST: если есть незакрытые вопросы уровня CRITICAL/ERROR/WARNING, нельзя вносить изменения, связанные с этими вопросами, до получения ответа. При этом можно продолжать работу по репозиторию в целом, если задачи не связаны с вопросами.
- SHOULD: если остались только INFO/DEBUG, нужно сказать, что они малой важности и можно автоматически принять рекомендованный вариант, и уточнить, нужно ли их задавать.

#### Примеры

Правильно (сжатый пример):

```
**Вопрос №1:**
    Какой формат логирования выбрать для нового сервиса?
**Критичность:**
    3. WARNING
**Варианты ответа:**
    | id | описание | рекомендовано |
    |---|---|---|
    | A | JSON | ✓ |
    | B | Plain text |  |
    | C | Syslog |  |
    | D | предложи свое коротко |  |
**Рекомендация:**
    JSON проще парсится и пригоден для централизованных систем логирования.
```

Неправильно:
- вопрос без критичности/вариантов;
- несколько вопросов в одном блоке;
- последний вариант не "предложи свое коротко".

## 2. Разработка

### 2.0 Архитектура и паттерны (MUST)

- MUST: держать архитектуру чистой — разделять ответственность по слоям/модулям, избегать лишних зависимостей и «знания» о деталях инфраструктуры.
- MUST: применять паттерны проектирования там, где они упрощают поддержку, расширение и тестирование.
- MUST: если был использован паттерн — сообщать пользователю какой именно и где (файлы/модули/слои).
- MUST: если в существующем коде видно, что должен быть паттерн, но его нет, — предложить пользователю вопросом «ленивый» рефакторинг этого места в рамках текущей задачи; описать причины, пользу и ожидаемые улучшения (читаемость, тестируемость, расширяемость, изоляция зависимостей).
- SHOULD: отдавать приоритет явным контрактам и инъекции зависимостей.
- Примеры паттернов:
  - (не ограничиваемся этим списком) — выбирать паттерн по контексту задачи.
  - Repository (предпочтительный): изоляция доступа к данным от доменной логики.
  - Service Layer: координация сценариев использования и транзакций.
  - Factory/Abstract Factory: создание сложных объектов без привязки к конкретным классам.
  - Strategy: взаимозаменяемые алгоритмы под единым интерфейсом.
  - Adapter: интеграция внешних API без утечек их деталей в домен.

### 2.1 Python (MUST)

#### Докстринги (MUST)

- MUST: документировать функции и классы в Google-style docstring.
- MAY: для очень коротких функций допустим однострочный docstring.
- MUST: если функция достаточно сложная (обычно >10 строк) или без описания параметров назначение непонятно — писать полный Google-style с `Args/Returns` и т. п.
- MUST: если у короткой функции уже есть полный Google-style docstring, не сокращать его.
- MUST: при замечаниях линтера к docstring (например, D205/D212/D411) исправлять форматирование, а не сокращать/удалять секции `Args/Returns`.

#### Как определить Python‑проект

- MUST: считать проект Python, если есть хотя бы один маркер: `pyproject.toml`, `requirements*.txt`, `setup.cfg`, `setup.py`, `Pipfile`, `poetry.lock`, `uv.lock`, или пользователь явно сказал, что это Python.

#### Менеджмент зависимостей и запуск

- MUST: использовать `uv` для управления пакетами и запуска команд.
- SHOULD: предлагать миграцию с pip/venv на uv, если проект legacy.
- Примеры:
  - `uv add package` — добавить зависимость в `pyproject.toml` (предпочтительно).
  - `uv pip install package` — установить в venv без записи.
  - `uv pip install -r requirements.txt` — для legacy‑проектов.

### 2.2 Безопасная разработка Python

#### Проверка инструментов (MUST)

- MUST: проверить, установлены ли `ruff` и `mypy` (в `pyproject.toml`, `requirements*.txt` или через установленные зависимости).

#### Если ruff/mypy установлены (MUST)

- MUST: `uv run ruff format`
- MUST: `uv run ruff check` (допустим `--fix` только для безопасных исправлений)
- MUST: `uv run mypy src/` или иной путь, заданный в конфиге проекта
- MUST: запускать форматирование/линтеры/тесты только если были изменения в коде; при изменениях только в конфигах/документации запуск не требуется.

Для тестов (`tests/`) допускается полное игнорирование правил:
- SHOULD: оформлять через конфиги (`ruff.per-file-ignores` для `tests/**`, `mypy.exclude` или `ignore_errors` для `tests/**`).

#### Если ruff/mypy НЕ установлены (MUST)

- MUST: явно предупредить пользователя и выделить текст жирным + курсивом (использовать `***…***`) о том, что проверки не запускались.
- MUST: предложить установить, настроить и исправить ошибки.
- SHOULD: спросить у пользователя список правил для ruff и наличие целевого конфига. Если нет — настроить самостоятельно и обосновать отключение каждого правила.

### 2.3 Тесты

- MUST: если в проекте есть тесты, каждую фичу/коммит покрывать юнит‑тестами.
- SHOULD: для фичей в приоритете интеграционные и e2e тесты.
- MUST: если при коммите/пуше отсутствуют более высокоуровневые тесты для фичи — предупредить пользователя.
- MUST: если тестов нет, или их очень мало — один раз спросить пользователя, нужно ли покрывать текущий код тестами, и предложить это сделать.

### 2.4 Фронтенд и скриншоты (MUST)

- MUST: после редактирования фронта в `.tmp` (создать локально в текущем репозитории, если отсутствует) складывать скриншоты UI именно в `.tmp`.
- MUST: в ответе пользователю давать **прямой путь к файлам** скриншотов, чтобы он мог открыть их локально.
- SHOULD: хранить скриншоты в подпапке `.tmp/screenshots/` с понятными именами.

Примеры:

Создание каталога и сохранение скриншота:
```bash
mkdir -p .tmp/screenshots
# пример: сохранить скриншот в .tmp/screenshots
```

Пример с Playwright (если установлен в проекте):
```bash
node -e "const { chromium } = require('@playwright/test');(async()=>{const b=await chromium.launch();const p=await b.newPage({viewport:{width:1440,height:900}});await p.goto('http://127.0.0.1:4300/tasks/action-items');await p.waitForTimeout(1000);await p.screenshot({path:'.tmp/screenshots/action-items-calendar.png',fullPage:true});await b.close();})();"
```

Пример ответа пользователю (обязательная ссылка на файл):
```
Скриншот: /absolute/path/to/repo/.tmp/screenshots/action-items-calendar.png
```
<!-- END: copied verbatim from /home/user/.codex/AGENTS.md -->
