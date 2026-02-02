---
name: kb-show-todos
description: 'Quick current tasks overview: refresh/read notes/work/daily-brief.md and summarize overdue/due-soon + top tasks + needs-review in 10–15 lines, then ask one question (top‑3). Triggered by /show-todos, "что по задачам", "что делать", "покажи задачи".'
---

# kb-show-todos

Use this skill when the user wants a fast overview of current tasks.

## Steps

1) Best-effort refresh:

```bash
.venv/bin/python scripts/kb.py day-start
```

- If Jira auth is missing/unavailable: tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`), then continue by reading the existing `notes/work/daily-brief.md` if present.

2) Summarize in **10–15 lines**:

- Overdue / due soon (if present)
- Top 5 tasks by actionability (In progress/Review first)
- “Needs Review” items (short)

Always include titles next to keys (avoid bare IDs).

3) Ask only one question if needed:

- “Что берём в топ‑3 на сегодня?”
