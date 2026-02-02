---
name: tg-topics-workflow
description: 'Work protocol for Telegram private topics/threads in this repo: topic = project/context (not 1 task), scope=(chat_id,message_thread_id), how to run multiple tasks sequentially in one topic (via /new), how to route follow-ups, and how to avoid creating extra topics. Triggered by "топики", "threads", "scope", "message_thread_id".'
---

# tg-topics-workflow (draft)

Use this skill when planning or implementing a **topic/thread-based** workflow for the Telegram bot (`tg_bot/`), or when you need to operate an ongoing task inside a specific Telegram topic.

## Core idea: scope = (chat_id, message_thread_id)

- Treat every Telegram topic/thread as an independent “project/context container” (not “1 task = 1 topic”).
- In private chats with topics enabled, scope key is: `scope = (chat_id, message_thread_id)` where `message_thread_id > 0`.
- One topic may contain multiple tasks sequentially; keep the topic name stable to reflect the context.

## Routing rules (agent behavior)

- If scope is **active** (`inflight` or `waiting_for_user`): do not start a new task; append the message as follow-up context for the same scope.
- If the user explicitly wants a **new task** inside the same topic while scope is active (esp. in `waiting_for_user`): use `/new <текст>` (new task, not an answer).
- If scope is **idle**: the next user message in that scope starts the next task for that scope.
- If a message arrives outside any topic (`message_thread_id` missing/0): treat it as “intake” and either:
  - ask the user to post into a topic, or
  - start a new scope using the message’s thread id once it exists (implementation detail; see tech note).

## Topic naming (auto)

- Bot may auto-rename a newly used topic on the first run.
- Naming rule: 1 emoji + 1–2 words (e.g., `🔧 Рефактор`).
- Keep the topic name stable: do not rename it per task; rename only to reflect the context.

## Parallelism (draft direction)

- Default mode can stay serialized (single queue), but the user may want “quick read task” to run while a long refactor is running in another topic.
- If parallel execution is enabled:
  - enforce “1 active job per scope” (avoid mixing contexts inside a topic),
  - allow multiple scopes to run concurrently up to a global limit,
  - expose explicit user controls (buttons) to run a queued task “now” or “in parallel”.

Keep this as a documented feature in the tech notes (no separate skill).

## Delivery rules (what to send where)

- **Progress telemetry**: keep a single “ack/progress” message per scope and edit it.
- **Final answer**: send in the same scope/topic, preferably as:
  - a short final message, or
  - a document (Markdown) if the output is long/structured.
- **Blocking questions** (need answer): post as a separate message in the same scope/topic (see `tg-user-in-the-loop`).
- **Live chatter** (no answer needed): separate short messages, strictly throttled (see `tg-live-chatter`).

## Avoid creating extra topics

- Always reply using the same `message_thread_id` as the user message/topic.
- Do not send “side” messages without `message_thread_id`, otherwise Telegram may create or use a different topic.

## References (repo)

- Design note: `notes/technical/2026-01-10-telegram-topics-threads-task-routing.md`
- Live chat protocol: `notes/technical/2026-01-10-tg-bot-live-chat-protocol.md`
