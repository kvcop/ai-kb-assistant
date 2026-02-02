# day-start

You are starting a new work day.

## Language

Communicate in the user's language; default to Russian.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Steps

1) Check local date/time:
- Run: `date '+%Y-%m-%d %H:%M (%A) %Z'`

2) Refresh the task brief from Jira:
- Run: `python3 scripts/kb.py day-start`
- If Jira sync is unavailable, it will fall back to the latest snapshot (or create a stub).
- If the error mentions missing auth: tell the user to create/update `.env` with:
  - `JIRA_URL="https://jira.<company>.ru"` (your Jira base URL)
  - `JIRA_TOKEN="..."` (personal access token)
  - `JIRA_USERNAME="you@company.com"` (optional; enables Basic auth)

3) Read and summarize:
- Read `notes/work/daily-brief.md`
- Summarize: overdue/soon, top candidates, and anything in “Needs Review”.
- Always include titles next to keys (avoid bare `RND-123` in chat): prefer `KEY — title — status` or `KEY (title) — status`.

4) Ensure today’s notes container exists:
- `python3 scripts/kb.py day-start` already calls `open-day`, but if `notes/work/end-of-day.md` is missing or has the wrong date, run: `python3 scripts/kb.py open-day`
- `open-day` also auto-injects issue titles into `Time Tracking (draft)` lines when possible (latest Jira snapshot + `notes/work/time-buckets.md`), so titles are kept even if Jira changes later.

5) Ask clarifying questions (only if needed):
- If “Needs Review” has items: ask to confirm theme mapping / intent.
- If tasks look stale or missing: ask if the Jira JQL in `configs/kb.toml` needs changes.

6) Confirm today’s plan:
- Ask for top‑3 priorities for today (or confirm the suggested top list).
- Ask if there are any meetings today and whether notes are expected.

## Output expectations

- Keep the message short: 1 screen.
- When mentioning Jira tasks, do not output bare keys: always attach the title (and status when listing).
- Update nothing manually unless the user provides new facts/notes.
