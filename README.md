# KB Template + Telegram Codex Bot

Этот репозиторий — OSS‑шаблон базы знаний: структура папок `notes/` + утилиты для ежедневного контура (`scripts/kb.py`) и Telegram‑бот‑мост к Codex (`tg_bot/`).

Здесь **нет личных данных**: только “рыба” и инструкции. Дальше вы заполняете `notes/` под себя.

## Что внутри

- `notes/` — база знаний (Markdown).
- `scripts/kb.py` — утилита для брифа/итогов/вопросов/тайм‑репорта.
- `tg_bot/` — Telegram ↔ Codex bridge + watcher/напоминания.
- `.codex/` — локальные промпты/skills для Codex CLI (опционально).

## Быстрый старт (без Telegram)

1) Проверьте зависимости:
- Python `>= 3.11`
- (опционально) `uv`

2) Сгенерируйте стартовые файлы дня:

```bash
python3 scripts/kb.py open-day
python3 scripts/kb.py day-start
```

3) В течение дня пишите заметки в `notes/work/end-of-day.md` и закрывайте день:

```bash
python3 scripts/kb.py end-day
python3 scripts/kb.py time-report --jira
```

## Запуск Telegram‑бота

### 1) Подготовить окружение

1) Установите Codex CLI так, чтобы был доступен бинарь `codex`.

2) Создайте бота у @BotFather → получите `TG_BOT_TOKEN`.

3) Создайте `.env.tg_bot`:

```bash
cp .env.tg_bot.example .env.tg_bot
${EDITOR:-nano} .env.tg_bot
```

Полный список переменных — в `tg_bot/examples/env.example`.

4) (Опционально) Jira:

```bash
cp .env.example .env
${EDITOR:-nano} .env
```

### 2) Запустить

Вариант без зависимостей (если хватает системного Python):

```bash
python3 -m tg_bot
```

Вариант через `uv` (рекомендуется):

```bash
uv venv .venv
.venv/bin/python -m tg_bot
```

### 3) Telegram topics + `/reminders` (обязательно для напоминаний)

1) Если вы используете **групповой чат**: включите **Topics** (форум) в настройках супергруппы и добавьте бота (лучше админом).

2) Откройте топик, куда бот должен присылать напоминания, и выполните команду:

```text
/reminders
```

Эта команда “привяжет” доставку напоминаний к текущему топику (chat_id + message_thread_id).

3) Редактируйте список напоминаний в `notes/work/reminders.md`.

## Codex CLI без Telegram (prompts + AGENTS)

- Инструкции для агента: `AGENTS.md`.
- Готовые промпты: `.codex/prompts/`.

Чтобы подключить промпты глобально для Codex CLI:

```bash
mkdir -p ~/.codex
ln -s "$PWD/.codex/prompts" ~/.codex/prompts
```

После этого можно запускать Codex прямо из папки репозитория — агент будет видеть `AGENTS.md` и сможет работать с базой знаний без Telegram‑бота.

## Локальный Telegram Bot API server (опционально)

Если вы хотите использовать локальный Telegram Bot API (например, чтобы меньше зависеть от внешней сети), есть шаблон systemd‑сервиса:

- `tg_bot/examples/telegram-bot-api.service`
- `tg_bot/examples/telegram-bot-api.env.example`

## Где что менять

- Jira JQL по умолчанию: `configs/kb.toml` и/или `notes/work/jira.md`
- Примеры `.env`: `.env.example`, `.env.tg_bot.example`, `tg_bot/examples/env.example`
- Структура `notes/`: см. `notes/README.md`

