# Codex Prompts

These prompt templates define the daily workflow for this repository.

Symlink into Codex CLI default location:

```bash
mkdir -p ~/.codex
ln -s "$PWD/.codex/prompts" ~/.codex/prompts
```

Prompts:
- `day-start.md` — morning routine (refresh Jira brief, ask clarifying questions)
- `show-todos.md` — quick view of current tasks
- `end-day.md` — end-of-day wrap-up (delta + time tracking draft)
- `log-notes.md` — mid-day notes/meeting capture
- `meeting-protocol.md` — generate a report-style meeting protocol from `tmp/` transcripts + move artifacts into KB
- `answer-questions.md` — batch-resolve pending questions (time buckets, needs-review, Jira hygiene)

Tip: if you store Jira creds in `.env`, you do **not** need to `source` it: `scripts/kb.py` auto-loads `JIRA_*` from `.env` on every run. If you see “Missing Jira auth”, create/update `.env` with `JIRA_URL` + `JIRA_TOKEN` (and optionally `JIRA_USERNAME`).
