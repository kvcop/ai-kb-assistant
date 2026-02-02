# log-notes

The user is sending mid-day notes (status update, meeting notes, decisions, action items).

## Language

Communicate in the user's language; default to Russian.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Goal

Capture notes with the right level of detail, without bloating the daily files.

## Steps

0) Ensure you write into the correct day container:
- Run: `date '+%Y-%m-%d %H:%M (%A) %Z'`
- If time is between 00:00–06:00 and it is unclear, ask: “Пишем в вчерашний день или сегодняшний?”
- Run: `python3 scripts/kb.py open-day` (or `--date YYYY-MM-DD`)

1) Ask for minimal metadata if missing:
- Which Jira issue key(s) does this relate to? (or “none”)
- Is this a meeting? If yes: meeting name + participants + outcomes.

2) Persist

- Always add a short bullet summary into `notes/work/end-of-day.md` under “Notes (keep short)”.
- If you created a dedicated note, add a link under “Links (optional)”.
- If the notes are long or meeting-like: create a dedicated file:
  - Meeting: `notes/meetings/YYYY-MM-DD-<slug>.md`
  - Technical note: `notes/technical/YYYY-MM-DD-<slug>.md`
  - Then link it from `notes/work/end-of-day.md`.

3) Track effort (time)

- If the user provided any time hints (e.g. `2h`, `30m`, `1ч 20м`) and related Jira keys — update `notes/work/end-of-day.md` → “Time Tracking (draft)” with those hours.
- If Jira keys are mentioned but no time is provided — ensure placeholders exist (e.g. `RND-123 — <название> — __h`) so they are not forgotten; do **not** spam questions.
- If time is mentioned without Jira keys — put it into “Other / misc” (or ask 1 clarification if it must be mapped).
- Never invent exact hours; use only user-provided hints or mark as `__h` for later confirmation at end-day.
- Prefer the canonical line format: `KEY — Название — 1:30 (что сделано; 11:00–12:30)` (or `KEY — Название — __h`).
- If you don’t know the title yet, it’s OK to write `KEY — 1:30 (...)` / `KEY — __h`; then run `python3 scripts/kb.py open-day` once more to auto-inject titles from the latest Jira snapshot + `notes/work/time-buckets.md`.

4) Extract action items

- Add a short “Action items” subsection to the dedicated note (or to end-of-day if no dedicated note is created).
- If something is unclear, ask 1–3 questions максимум.

## Output expectations

- Keep `notes/work/end-of-day.md` concise; push details into a dedicated note file when needed.
- When replying in chat and you mention Jira tasks, use `KEY — title` (or `KEY (title)`) instead of bare keys whenever the title can be resolved from the daily brief/snapshot.
