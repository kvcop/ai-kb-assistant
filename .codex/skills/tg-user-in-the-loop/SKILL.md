---
name: tg-user-in-the-loop
description: 'Async clarifying questions during task execution in tg_bot (user-in-the-loop): ask blocking questions, wait in same topic, ping on 5/10/15 minute schedule, continue with defaults (read/write only) or cancel if no default. Use /new to start a new task instead of answering. Triggered by "уточни", "спроси в процессе", "жду ответ", "таймаут", "user-in-the-loop".'
---

# tg-user-in-the-loop (draft)

Use this skill when you need **clarifications while the task is still running** and you want the bot to keep the task “open” (not finalized) while waiting for the user.

## Message types

- **Blocking question**: requires user input to proceed safely.
- **Non-blocking note**: useful information, no reply required.

## How to ask a blocking question (format)

Keep it short and actionable:

1) 1-line context: why it matters.
2) 1–3 questions max.
3) A default assumption if the user doesn’t answer.

Template:
```
❓ Нужен ответ, иначе риск/переделка: <1 строка>
1) <вопрос A/B?>
2) <вопрос>
Если не ответишь — через 5 минут продолжаю так: <дефолт>.
```

## Timeout policy (expected bot behavior)

Target behavior (draft, to implement in `tg_bot/`):

- After `T+5m` since asking: ping #1.
- After `T+10m`: ping #2.
- After `T+15m`: ping #3 (last).
- After the last ping (~15 minutes):
  - If `default` is set and mode is `read/write`: auto-continue with the default.
  - If `default` is NOT set: cancel the task and ask the user to restart later (use `/new <текст>`).
  - If mode is `danger`: never auto-continue (only explicit user answer).

## UX notes

- If options are provided, prefer offering 3–5 tap-buttons (A/B/…) but always allow a text answer as fallback.
- While waiting for an answer, a user may start a new task in the same topic with `/new <текст>` (it cancels waiting).

## Important constraints

- Avoid asking multiple separate blocking questions; batch them.
- Prefer “continue with safe default” over blocking when possible.
- The answer must be captured as a new fact and included in the next continuation step.

## References (repo)

- Live chat protocol: `notes/technical/2026-01-10-tg-bot-live-chat-protocol.md`
