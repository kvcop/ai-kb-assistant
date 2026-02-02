---
name: kb-answer-questions
description: 'Batch-resolve pending questions/missing info: run python3 scripts/kb.py questions (optionally for a date), ask the user to confirm time tracking placeholders, needs-review items, Jira hygiene, and manual open questions; then apply answers by updating notes/work/end-of-day.md and notes/work/open-questions.md. Triggered by /answer-questions, "могу ответить на вопросы", "разобрать вопросы".'
---

# kb-answer-questions

Use this skill when the user is ready to answer questions / resolve missing info.

## Steps

1) Anchor time:

```bash
date '+%Y-%m-%d %H:%M (%A) %Z'
```

2) Generate the pending questions report:

```bash
.venv/bin/python scripts/kb.py questions
```

If the user specified a target day (`YYYY-MM-DD`):

```bash
.venv/bin/python scripts/kb.py questions --date YYYY-MM-DD
```

3) Ask the user to resolve items in batch (minimal interaction):

- Time tracking: ask for hours only where `__h` is present.
- Needs review: ask what to do (confirm theme mapping / ignore for now).
- Jira hygiene: ask whether to update status/comment (yes/no) for listed keys.
- Manual open questions: answer/close them or keep open.

4) Apply answers:

- Update `notes/work/end-of-day.md` → “Time Tracking (draft)” with confirmed hours (do not invent).
- Move resolved items from `notes/work/open-questions.md` → “Resolved (log)” with a short trail.

## Output expectations

- Return a short checklist: what was resolved vs what remains open.
