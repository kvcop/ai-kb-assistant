# Codex Skills (Repo-local)

These are **repo-local Agent Skills** for this repository.

Location: `.codex/skills/*/SKILL.md`

## Skills

- `kb-core` — repo operating rules: layout, safe `scripts/kb.py` usage, time tracking conventions.
- `kb-memory` — persistent MCP memory policy (server-memory): preferences, people/roles, pointers to KB notes.
- `kb-day-start` — morning routine: refresh brief + summarize priorities.
- `kb-show-todos` — quick “what should I do now” list.
- `kb-log-notes` — capture mid-day notes into EOD + optional dedicated note.
- `kb-log-ideas` — capture raw ideas into `notes/ideas/` (TL;DR + raw transcript; keep open-questions clean).
- `kb-meeting-protocol` — build meeting protocol from `tmp/` transcripts + move artifacts into KB.
- `kb-answer-questions` — batch-resolve pending questions (`kb.py questions`).
- `kb-end-day` — end-of-day summary + time report (+ optional week-to-date).
- `kb-orchestrator-workflow` — orchestrator-first workflow: stages + artifacts + review/fix loop + Playwright MCP for UI checks.
- `codex-access-escalation` — escalate permissions/network access (Telegram: ask for `∆` re-send; non-Telegram: Codex flags + resume).
- `tg-bot-usage` — operate the Telegram bot UX (queue/settings/attachments), including sending files/archives back via `/upload`.
- `tg-bot-dev` — develop and maintain `tg_bot/` (commands, queue/control plane, keyboards, state, delivery).
- `tg-topics-workflow` — Telegram topics/threads workflow (private topics): scope routing + where to post progress/final.
- `tg-user-in-the-loop` — async clarifications during task execution (blocking questions + 5/15m timeouts + defaults).
- `tg-live-chatter` — short human-like status updates while working (strict throttling, no reply required).
- `tg-mcp-send-files` — send files to Telegram via MCP `telegram-send` (async queue + retries), especially multi-file batches.
