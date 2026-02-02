---
name: kb-orchestrator-workflow
description: "Orchestrator-first workflow for building and shipping changes via KB Orchestrator (orchestrator/): setup target repo/workdir, run validation jobs with artifacts + safety, then run structured code review and apply fixes (requesting-code-review/receiving-code-review), and verify web UI behavior via Playwright MCP. Use when the user asks “через оркестратор”, wants repeatable stages + audit trail (commands/prompt), or needs phone/VPN-accessible web demo checks."
---

# KB Orchestrator Workflow

## Цель
Сделать выполнение задач **через оркестратор** воспроизводимым: чёткие этапы → артефакты → проверки → ревью → фиксы → (для UI) Playwright‑проверка → финальный отчёт.

## Codex-like Tasks (продуктовый режим)
- Пользователь ставит задачу как `Task` (repo + base ref + prompt + `N` версий).
- Каждая версия (`TaskVersion`) живёт в **своём изолированном окружении** (у нас: отдельный `worktree/workspace` на worker’е).
- Запуск “как в Codex”: `Task` сразу попадает в очередь, а worker **сам** берёт следующее из очереди (без ручного “run-next”/кнопок запуска воркера).
- Пайплайн стадий (MVP): `research? → execute → tests → review → fix`.
  - `research` **опционален**: orchestrator решает, нужен ли он по контексту/задаче.
  - `tests` может включать “run/smoke/Playwright” для веб‑приложений → обычно это делает `tester` с `capability=network`.
  - “user‑facing deploy” (URL/порт для пользователя) — отдельный **опциональный** Job/подстадия, включаемая только при явной необходимости.
  - `review` должен быть **read-only** (например, `codex exec -s read-only ...`), результат — отчёт, а не изменения.
  - `fix` применяет выводы ревью; при необходимости повторить `tests`.
- UX “версий”:
  - Код-ревью: параллельно запустить `N` версий, собрать фидбэк и агрегировать уникальное.
  - Креатив: выбрать лучшую версию/вариант и продолжать только её (или разветвить выбранную ещё на `2–4`).

## Связанные навыки (использовать как подпроцессы)
- Code review request: `.codex/skills/requesting-code-review/SKILL.md`
- Code review apply: `.codex/skills/receiving-code-review/SKILL.md`
- Frontend дизайн: `.codex/skills/frontend-design/SKILL.md`
- Отправка файлов в Telegram: `.codex/skills/tg-mcp-send-files/SKILL.md`

## Инварианты (safety)
- Не делать `git push` без явного запроса пользователя.
- Не использовать `sudo`.
- Держать рабочее дерево чистым: всё нужное — закоммитить, рантайм/артефакты — в `.gitignore`.
- Для “executor без сети”: **не полагаться на удачу**. Если нужен интернет — добывать материалы control‑plane’ом и передавать локально.

## Артефакты (канонично)
- В целевом проекте держать `ORCH_RUN.md`: старт/финиш, исходный prompt, точные команды, ссылки, итерации, job_id.
- В оркестраторе использовать артефакты `orchestrator/state/artifacts/<job_id>/summary.json` и `iterations.json` как источник правды по времени/итерациям.

## Этапы (строго по порядку)

### 0) Зафиксировать вводные
- Зафиксировать `pwd` и целевую папку работы (если нужно “не захламлять” — подняться уровнем выше и работать рядом).
- Создать/обновить `ORCH_RUN.md` (в целевом репо): `Start`, цель, URL (если UI), критерии DoD.
- Сформулировать **DoD** и stop‑условие (по умолчанию `COMPLETED` строго последней строкой stdout).

### 1) Подготовить целевой репозиторий
- Создать папку/репо, настроить git‑identity (локально).
- Добавить `.gitignore`, базовый `README.md`, тестовый каркас, `ruff`/`mypy`.
- Прогнать проверки локально и сделать baseline commit.

### 2) Сформировать prompt для executor (качество постановки)
- Явно перечислить: ограничения (no push/sudo), DoD, команды проверок, артефакты.
- Если задача про UI: указать URL и требование проверить сценарии через Playwright MCP.
- Встроить “loop hook”: `COMPLETED` (и опционально `BLOCKER`) как последняя строка stdout.
- Записать prompt в `ORCH_RUN.md` (как “исходный запрос”).

### 3) Выполнить реализацию (executor)
- Делать маленькие, проверяемые шаги.
- Коммитить осмысленными порциями; не оставлять мусор/временные файлы вне `.gitignore`.
- Не менять тесты executor’ом (если политика включена) — для тестов использовать отдельный этап/роль.

### 4) Прогнать проверки через KB Orchestrator (валидировать и собрать артефакты)
- Зарегистрировать repo:
  - `cd orchestrator`
  - `../.venv/bin/python -m kb_orchestrator repo add --id <id> --path <abs_path>`
- Создать job на проверки (пример):
  - `../.venv/bin/python -m kb_orchestrator job create --repo <id> --max-iters 3 --completion-keyword COMPLETED -- bash -lc '<checks...>; echo COMPLETED'`
- Запустить worker:
  - `../.venv/bin/python -m kb_orchestrator worker run` (по умолчанию в цикле; `--once` чтобы выполнить один job)
- Если `blocked`:
  - “грязное дерево” → создать отдельную cleanup‑подзадачу (commit/ignore), без авто‑удалений.
  - “executor modified tests” → откатить/перенести изменения тестов в отдельный этап.

### 5) Создать агента на code review (requesting-code-review)
- В отдельном запуске/топике попросить review по `{BASE_SHA}..{HEAD_SHA}`.
- Следовать `.codex/skills/requesting-code-review/SKILL.md`.
- Сохранить результат в целевом репо (например `CODE_REVIEW.md`) и залинковать из `ORCH_RUN.md`.

### 6) Создать агента на фиксы по ревью (receiving-code-review)
- В отдельном запуске/топике применить рекомендации, **проверяя техническую корректность**.
- Следовать `.codex/skills/receiving-code-review/SKILL.md`.
- После каждого батча фиксов повторить этап 4 (оркестратор‑проверки).

### 7) Для UI: проверить поведение через Playwright MCP
- Поднять сервер на `0.0.0.0` (и зафиксировать PID/лог/stop‑команду в `ORCH_RUN.md`).
- Проверить сценарий “как пользователь” через MCP Playwright:
  - `mcp__playwright__browser_navigate` → URL (например `http://192.168.77.4:8000`)
  - `mcp__playwright__browser_snapshot` → зафиксировать состояние
  - при проблемах: `mcp__playwright__browser_console_messages`, `mcp__playwright__browser_network_requests`
- Если найдены ошибки/несостыковки: создать fix‑подзадачу и повторить 4 → 7.

### 8) Финализировать и отчитаться
- Обновить `ORCH_RUN.md`:
  - время (start/end), итерации, job_id, список команд, исходный prompt, ссылку на результат.
- В ответ пользователю всегда дать:
  1) сколько времени заняло
  2) сколько итераций (по `iterations.json`)
  3) `.md` с командами+prompt
  4) ссылку/URL
  5) доп. заметки/риски/ограничения
- Отправить `ORCH_RUN.md` в Telegram документом через MCP (см. `.codex/skills/tg-mcp-send-files/SKILL.md`).
