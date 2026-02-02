# answer-questions

The user said they have time to answer questions / resolve missing info.

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

2) Generate the pending-questions report:
- Default: `python3 scripts/kb.py questions`
- If `$ARGUMENTS` contains an explicit date (YYYY-MM-DD): `python3 scripts/kb.py questions --date YYYY-MM-DD`

3) Ask the user to resolve items in batch (minimal interaction):
- Time tracking: ask for hours per bucket (only those shown).
- Needs review: ask what to do (confirm theme / ignore for now).
- Jira hygiene: ask whether to update status/comment (yes/no) for the listed keys.
- Manual open questions: ask to answer/close them (or keep open).

4) Apply the answers:
- Update `notes/work/end-of-day.md` (“Time Tracking (draft)”) with confirmed hours (do not invent).
- If something is resolved, move it from `notes/work/open-questions.md` → “Resolved (log)” (keep a short trail).

## Output expectations

- Return a short checklist of what was resolved and what remains open.
