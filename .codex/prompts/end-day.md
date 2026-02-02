# end-day

You are closing the work day and preparing a clear breakdown + time tracking draft.

## Language

Communicate in the user's language; default to Russian.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Steps

0) Determine report mode from `$ARGUMENTS`:
- Default: day
- If user asks for a weekly report (`неделя`, `week`, `weekly`) → produce week-to-date summary (Mon..closed day)

1) Determine which day to close:
- Run: `date '+%Y-%m-%d %H:%M (%A) %Z'`
- If time is between 00:00–06:00, ask whether to close “yesterday” or “today”.

2) Refresh Jira delta + end-of-day file:
- If closing a specific day: `python3 scripts/kb.py end-day --date YYYY-MM-DD`
- Otherwise: `python3 scripts/kb.py end-day`
- If the output mentions missing Jira auth: tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`). The command will still try to use the latest snapshot if available.

3) Read and complete:
- Read `notes/work/end-of-day.md`
- Ensure “Notes (keep short)”, “Links (optional)”, and “Time Tracking (draft)” reflect today’s work.
- Read `notes/work/open-questions.md` and confirm carryover merged into the next workday (no duplicates; dates updated if needed).

4) Ask for missing information (only if needed):
- What meetings happened today and outcomes?
- If time tracking is incomplete: ask for a rough time split across 2–5 buckets (by Jira issue or theme).

5) Produce the end-of-day output:
- Short breakdown of what moved (by Jira keys and status changes).
- Always generate a time report from notes: `python3 scripts/kb.py time-report` (use `--date YYYY-MM-DD` if you closed a specific day).
- If there is enough information in notes: output “куда и сколько времени ушло” in a **worklog-friendly** way:
  - list **each time entry line** separately (even if they share the same Jira key), with the exact format: `KEY (название задачи) — время — что сделано`;
  - then show totals (sum) + any missing (`__h`) to confirm.
- Never output bare Jira keys in the breakdown: attach titles where possible (use the `time-report` / latest snapshot / daily brief as the source of truth).
- If there are missing buckets: ask the user to confirm/correct missing hours (do not invent).

6) Weekly time report (only if requested in `$ARGUMENTS`):
- Run: `python3 scripts/kb.py time-report --week` (week-to-date; use `--date YYYY-MM-DD` if you closed a specific day)
- Summarize totals and top buckets; ask for confirmation only if there are missing hours.

7) Commit today's work:
- Run: `git status --porcelain=v1` and ensure there are no secrets staged (especially `.env`).
- Stage and commit everything done today (notes + code): `git add -A && git commit -m "chore: end day YYYY-MM-DD"`.

## Persistence

- Keep history: do not delete prior daily logs if present.
