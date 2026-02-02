---
name: tg-bot-usage
description: Use the Telegram bot effectively (commands, queue, attachments), including sending files/archives back via /upload.
---

# tg-bot-usage

Use this skill when you need to operate the Telegram bot UX (or suggest copy-ready commands to the user), especially for returning artifacts back to Telegram.

## File upload (back to Telegram)

When the user asks for a file/log/archive (or you produced an artifact on disk), use `/upload`:

- `/upload <path>` — send a file as a Telegram document.
- If `<path>` is a **directory** — it will be zipped automatically.
- `/upload --zip <path>` — force zip even for a file.

Constraints:
- Path must be inside the repo workspace or `tg_uploads/**` (paths outside are rejected).
- Size limit: `TG_SEND_MAX_MB` (defaults to `TG_UPLOAD_MAX_MB`; fallback `50`).
- Owner private chat only (not for groups / non-owner chats).

Examples:
```text
/upload logs/tg-bot/state.json
/upload logs/tg-bot --zip
/upload tg_uploads/242753904/20260104-130256_827874_BQACAgIA_MVP_Инструкция_к_Ферме__11.11.pdf
```
