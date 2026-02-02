---
name: kb-day-start
description: 'Morning routine for this repo: refresh Jira snapshot and generate notes/work/daily-brief.md (python3 scripts/kb.py day-start), ensure today''s end-of-day container exists, summarize priorities, and ask minimal clarifying questions. Triggered by /day-start, "бриф", "план на день", "утро", "доброе утро".'
---

# kb-day-start

Use this skill when the user starts a new work day or asks for a morning brief.

## Steps

1) Anchor time:

```bash
date '+%Y-%m-%d %H:%M (%A) %Z'
```

2) Refresh the brief from Jira (best-effort):

```bash
.venv/bin/python scripts/kb.py day-start
```

- If Jira auth is missing/unavailable: tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`). Continue by reading the existing `notes/work/daily-brief.md` / latest snapshot if present.

3) Read and summarize `notes/work/daily-brief.md`:

- Overdue / due soon (if present)
- High priority / In progress / Review first; then “Сделать/Backlog”
- “Needs Review” items
- Reminders surfaced by the brief (if present)

In every task line, include title next to key (avoid bare IDs).

4) Ensure today’s end-of-day container exists:

- `day-start` already calls `open-day`, but if `notes/work/end-of-day.md` is missing or has the wrong date, run:

```bash
.venv/bin/python scripts/kb.py open-day
```

5) Ask clarifying questions only if needed:

- Confirm top‑3 priorities for today.
- If “Needs Review” has items: ask to confirm theme mapping / intent.
- If tasks look stale or missing: ask whether the Jira JQL in `configs/kb.toml` needs changes.

## Output expectations

- Keep the response short (about 1 screen).
- Do not manually edit `notes/work/daily-brief.md`.
- Do not invent statuses, time spent, or outcomes.
