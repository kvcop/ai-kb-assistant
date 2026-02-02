---
name: tg-mcp-send-files
description: Send one or multiple local files to Telegram via MCP server `telegram-send` (async queue + retries). Triggered by "пришли мне", "скинь", "отправь", "документом", "файлом", "архивом", "mcp", "telegram-send", "sendDocument".
---

# tg-mcp-send-files

Use this skill when the user wants files sent to Telegram **via MCP** (not via bot `/upload`), especially when multiple files should be sent with one caption and delivery can be async with retries.

## Preferred tool

- Use `mcp__telegram-send__send_files` (async). It queues the files and returns OK immediately.
- Optionally call `mcp__telegram-send__queue_status` to confirm the queue is not stuck.

## Input contract (recommended)

- Pass multiple files at once:
  - `paths`: list of repo-relative or absolute paths
  - `caption`: one caption for the batch
- If running in Telegram bot context and the user writes from a topic/thread, send back into the same place:
  - pass `chat_id` and `message_thread_id` from the current context
  - if `message_thread_id` is missing/0, omit it (plain chat)

Batch delivery semantics:
- If `len(paths) <= 10`, sender tries to deliver them as a **single Telegram album** (`sendMediaGroup`) so it looks like “one message”.
- If the album send fails repeatedly (network/upload flakiness), it may fallback to “caption message + отдельные документы”.

Example tool call payload:
```json
{"chat_id":123,"message_thread_id":456,"paths":["notes/technical/a.md","notes/technical/b.md"],"caption":"Документы на проверку"}
```

## Notes

- If Telegram upload is flaky, do not retry in the main agent loop manually; rely on the MCP sender queue (max retries default 100 with backoff).
- If a file path is outside the current repo, prefer absolute paths.
