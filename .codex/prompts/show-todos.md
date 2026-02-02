# show-todos

Quickly show the current assigned tasks list (Jira is a helper; the repo is the source of truth for daily state).

## Language

Communicate in the user's language; default to Russian.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Steps

1) Try to refresh the brief:
- Run: `python3 scripts/kb.py day-start`
- If Jira auth is missing, fall back to reading the existing `notes/work/daily-brief.md` (if it exists) and tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`) for live Jira.

2) Summarize in 10–15 lines:
- Overdue / due soon (if present)
- Top 5 tasks by priority
- “Needs Review” items
- In every task line, include the title next to the key (avoid bare `RND-123`); use the wording from `notes/work/daily-brief.md` when possible.

3) Ask only one question if needed:
- “Что берём в топ‑3 на сегодня?”
