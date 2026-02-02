# meeting-protocol

You are preparing a meeting protocol from transcripts in `tmp/` and moving artifacts into the knowledge base.

## Language

Communicate in the user's language; default to Russian. The protocol itself MUST be in Russian.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Steps

1) Anchor time:
- Run: `date '+%Y-%m-%d %H:%M (%A) %Z'`

2) Inspect input transcripts:
- Run: `ls -la tmp`
- If `$ARGUMENTS` mentions a specific meeting, filter by it (substring match in filenames is OK).
- If `$ARGUMENTS` does **not** specify which meeting: process **everything** in `tmp/` (all meetings you can confidently identify).

3) Validate transcripts per meeting:
- A meeting MUST have a Voice Recognition transcript file.
  - Detect VoiceRec by content header `TEXT| TIME| SPEAKER` (not by filename only).
  - If there is no VoiceRec file: stop and ask the user what to do.
  - If there is a suspicious file like `<uuid>.txt` and it's not obvious whether it is VoiceRec:
    - ask the user to confirm it is VoiceRec or to rename it accordingly.
- Kontur Talk transcript is optional (if present, use it for speaker names; if absent, keep `SPEAKER_XX` in attributions and mention the limitation).

4) Ask clarifying questions (only if needed):
- If the user asked for a specific subset (e.g. “после демо”): confirm boundaries (what to include/exclude) **before** generating the final protocol text.
- If you encounter unclear terms/typos: collect them for later with **context** (do not guess silently):
  - Format: `term:proposed_fix` + 1 exact quote (1–2 lines around the term) + 1 short note “о чём речь”.

5) Generate artifacts (per meeting):
- Create `notes/meetings/artifacts/<YYYY-MM-DD-<slug>>/`.
- Write:
  - `kontur-tolk-transcription.md` (raw; names of speakers; add H1 + date + source note at the top)
  - `voice-rec-transcription-raw.md` (raw; may be out of order; add H1 + date + processor link at the top)
  - `voice-rec-transcription.md` (sorted by start time; derived from raw)
- Then move/remove the corresponding source files from `tmp/` (so `tmp/` stays temporary).

6) Generate meeting note (per meeting):
- Create `notes/meetings/YYYY-MM-DD-<slug>.md` (use the style of existing meeting notes).
- Include artifacts links.
- Include a **report protocol block** either at the very top or the very bottom of the file (easy copy/paste), in this exact structure:

Тема.

Коротко о встрече.
(2–4 предложения без буллетов)

Обсуждаемые темы.
(1–5 абзацев по 2–4 предложения; буллеты допустимы, но в основном текст)

Итоги
(1–2 абзаца по 2–4 предложения; буллеты допустимы)

Следующие шаги/Договоренности

* Задача
  * Ответственный
  * Сроки

7) Update typo glossary:
- Before adding each new typo, check if it already exists (limit output to ~5–10 lines):
  - Run: `python3 scripts/kb.py typos --query "TYPO" | tail -10`
- Add newly recognized typos/fixes into `notes/work/typos.md` (keep it sorted; avoid duplicates).
- Prefer using the helper: `python3 scripts/kb.py typos --add "TYPO" "FIX"` (repeat as needed).

8) Output to the user:
- By default, do **not** paste the whole protocol if it is already copy-ready in the meeting note; print only the file path(s) to `notes/meetings/YYYY-MM-DD-<slug>.md`.
- Print the unclear terms/typos with context for confirmation (format from step 4).
