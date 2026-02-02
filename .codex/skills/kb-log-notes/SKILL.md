---
name: kb-log-notes
description: Capture mid-day notes/decisions/action items into the correct day container (notes/work/end-of-day.md) without bloating it; create dedicated notes (notes/meetings, notes/technical, notes/work) when needed; update Time Tracking (draft) only from user-provided hints. Triggered by /log-notes, "запиши", "зафиксируй", "добавь заметку".
---

# kb-log-notes

Use this skill when the user sends mid-day notes (status update, meeting notes, decisions, action items).

## Steps

0) Ensure you write into the correct day container:

```bash
date '+%Y-%m-%d %H:%M (%A) %Z'
.venv/bin/python scripts/kb.py open-day
```

- If the user provided an explicit date (`YYYY-MM-DD`), use `.venv/bin/python scripts/kb.py open-day --date YYYY-MM-DD`.
- If it is between **00:00–06:00** and unclear, ask whether to log into yesterday or today.

1) Ask for minimal metadata only if missing:

- Which Jira issue key(s) does this relate to? (or “none”)
- Is this a meeting/sync? If yes: meeting topic + participants + outcomes.

2) Persist notes without bloating `end-of-day.md`:

- Always add a short bullet summary into `notes/work/end-of-day.md` under “Notes (keep short)”.
- If the content is long or meeting-like, create a dedicated note and link it under “Links (optional)”:
  - Meeting: `notes/meetings/YYYY-MM-DD-<slug>.md`
  - Technical deep dive: `notes/technical/YYYY-MM-DD-<slug>.md`
  - Small structured work note: `notes/work/YYYY-MM-DD-<slug>.md`

Dedicated note conventions:
- Start with an H1 and a `Date: YYYY-MM-DD` line.
- Add an “Action items” checkbox section if there are follow-ups.

3) Track effort (Time Tracking draft):

- Update `notes/work/end-of-day.md` → “Time Tracking (draft)” only from user-provided hints.
- If Jira keys are mentioned but time is unknown, keep placeholders:
  - `KEY — <название?> — __h`
- Prefer format:
  - `KEY — Название — 1:30 (что сделано; 11:00–12:30)`
  - `KEY — Название — __h`
- If the title is unknown, it is OK to write `KEY — __h`, then re-run `.venv/bin/python scripts/kb.py open-day` to auto-inject titles when possible (latest snapshot + `notes/work/time-buckets.md`).

4) Handle open questions:

- If something is unclear and the user wants to answer later, add it into `notes/work/open-questions.md` (Active) instead of looping in chat.

5) Optional: write to MCP memory (if allowed)

Only if the message contains a durable preference/decision/person-role mapping (apply `kb-memory` policy):
- update `User:<name>` preferences via `mcp__server-memory__add_observations` (or `delete_observations` + add if changed), OR
- create/update `Person:<...>` role/context (search → create entity if missing → add_observations), OR
- create a `Note:<path>` pointer to the dedicated KB note (create entityType `Note` + add `pointer:` observation; optional relations).

Otherwise: do not write to memory.

## Output expectations

- Confirm what was written where (file paths).
- Do not invent time, titles, or outcomes; ask or use `__h` / `<название?>`.
