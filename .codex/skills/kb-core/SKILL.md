---
name: kb-core
description: Operating rules and repo layout for the RND KB knowledge base. Use when deciding where to write notes (work vs meetings vs technical), how to run scripts/kb.py safely, or how to follow the time tracking conventions in notes/work/end-of-day.md.
---

# kb-core

Use this skill as a lightweight operating manual for this repository.

## Principles

- The knowledge base (this repo) is the source of truth for daily state.
- Jira is auxiliary: use it to refresh the brief and detect drift.
- Keep the active surface small: prefer generated state + tiny manual edits.
- Keep work vs personal separate: only write into `notes/personal/**` when the user explicitly says it is personal.

## MCP memory (server-memory)

- Use `kb-memory` policy when long-term context is needed (preferences, people/roles, pointers to KB notes).
- Do not duplicate daily work state in MCP memory: `notes/work/**` remains the source of truth.
- Safety: if Telegram context says `kb_scope: isolated (per-chat)` or group/non-owner, do NOT read/write MCP memory.
- Prefer `mcp__server-memory__search_nodes` + `mcp__server-memory__open_nodes` over `mcp__server-memory__read_graph`.

## Anchor time (always first)

```bash
date '+%Y-%m-%d %H:%M (%A) %Z'
```

If it is between **00:00–06:00** and the user’s intent is ambiguous, ask:
- “Пишем в вчерашний день или сегодняшний?”

## Jira and `.env`

- Do **not** `source` `.env`.
- `.venv/bin/python scripts/kb.py ...` auto-loads `JIRA_*` from `.env` best-effort (without overriding existing env).
- If Jira auth is missing/unavailable, tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`).

## Repository map (what matters most)

Active work surface (generated + lightly edited):
- `notes/work/daily-brief.md` — morning brief (generated)
- `notes/work/end-of-day.md` — end-of-day delta + short notes + time tracking draft
- `notes/work/open-questions.md` — deferred questions/blocks to batch-resolve later
- `notes/work/jira.md` — canonical JQL (generated from `configs/kb.toml`)

Long notes (link from end-of-day):
- `notes/meetings/` — meeting notes + `notes/meetings/artifacts/` for raw exports
- `notes/technical/` — technical deep dives

Off-limits by default:
- `archive/legacy/uba-ruma/` — read-only reference unless explicitly requested

## Time tracking conventions (draft)

- Never invent hours; if unknown, keep `__h` and ask later (batch via `kb.py questions`).
- Prefer line format:
  - `KEY — Название — 1:30 (что сделано; 11:00–12:30)`
  - `KEY — Название — __h`
- If one Jira key has multiple distinct topics, write multiple separate lines.
- When mentioning Jira tasks in chat, avoid bare keys: include `KEY — title` (or `KEY (title)`).

## Canonical commands (repo root)

```bash
.venv/bin/python scripts/kb.py doctor
.venv/bin/python scripts/kb.py day-start
.venv/bin/python scripts/kb.py open-day --date YYYY-MM-DD
.venv/bin/python scripts/kb.py end-day --date YYYY-MM-DD
.venv/bin/python scripts/kb.py questions --date YYYY-MM-DD
.venv/bin/python scripts/kb.py time-report --date YYYY-MM-DD
.venv/bin/python scripts/kb.py time-report --week --date YYYY-MM-DD
.venv/bin/python scripts/kb.py jira-issue RND-44
.venv/bin/python scripts/kb.py typos --query "..."
.venv/bin/python scripts/kb.py reminders
```
