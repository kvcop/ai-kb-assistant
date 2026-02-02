---
name: kb-end-day
description: 'Close the work day: run python3 scripts/kb.py end-day (optionally --date), review/complete notes/work/end-of-day.md, ensure carryover into notes/work/open-questions.md is clean, and generate a worklog-friendly time report (python3 scripts/kb.py time-report) with optional week-to-date summary. Triggered by /end-day, "конец дня", "итоги дня", "time report".'
---

# kb-end-day

Use this skill when the user is closing the day or wants an end-of-day/weekly report.

## Steps

0) Determine report mode:

- Default: day
- If the user asks for a weekly report (`неделя`, `week`, `weekly`) → also produce week-to-date summary.

1) Determine which day to close:

```bash
date '+%Y-%m-%d %H:%M (%A) %Z'
```

- If it is between **00:00–06:00** and the user did not provide a date, ask whether to close yesterday or today.

2) Refresh Jira delta + end-of-day file:

```bash
.venv/bin/python scripts/kb.py end-day
```

If closing a specific day:

```bash
.venv/bin/python scripts/kb.py end-day --date YYYY-MM-DD
```

- If Jira auth is missing/unavailable: tell the user to create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`). Continue with the latest snapshot if available.

3) Read and complete `notes/work/end-of-day.md`:

- Keep “Notes (keep short)” to 3–7 bullets.
- Put detail into a dedicated note and link it under “Links (optional)”.
- Never invent time: keep `__h` placeholders when unknown.

4) Carryover hygiene:

- Read `notes/work/open-questions.md` and ensure:
  - resolved items are moved to “Resolved (log)”
  - carryover from TODO / unchecked bullets is not duplicated (dates rolled forward as needed)

5) Generate time report output for the user:

```bash
.venv/bin/python scripts/kb.py time-report
```

If closing a specific day:

```bash
.venv/bin/python scripts/kb.py time-report --date YYYY-MM-DD
```

User-facing output should include:
- Worklog-friendly list of entries: `KEY (название задачи) — время — что сделано` (one line per entry)
- Totals + any missing `__h` for confirmation

6) Weekly time report (only if requested):

```bash
.venv/bin/python scripts/kb.py time-report --week
```

(Use `--date YYYY-MM-DD` if needed.) Summarize totals and top buckets; ask for confirmation only if there are missing hours.

7) Commit (only with explicit user intent):

- Do not auto-commit unless the user explicitly asked to commit/push.
