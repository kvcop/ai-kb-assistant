---
name: kb-meeting-protocol
description: 'Build meeting protocols from transcripts in tmp/: validate inputs (Voice Recognition transcript required), generate artifacts under notes/meetings/artifacts/, create a meeting note under notes/meetings/ (YYYY-MM-DD-slug.md) with a copy-ready Russian protocol block, and update notes/work/typos.md via kb.py typos. Triggered by /meeting-protocol, "протокол встречи", "разобрать транскрипт".'
---

# kb-meeting-protocol

Use this skill when the user asks to generate a meeting protocol/report from transcripts in `tmp/` and move artifacts into the knowledge base.

## Notes on sources

- “Speech2Text” refers to an optional speech-to-text service. Configure access via `scripts/speech2text.py`
  (base URL via `SPEECH2TEXT_BASE_URL`, token via `SPEECH2TEXT_TOKEN` or `~/.config/speech2text/token`).

## Steps

1) Anchor time:

```bash
date '+%Y-%m-%d %H:%M (%A) %Z'
```

2) Inspect input transcripts:

```bash
ls -la tmp
```

- If the user specifies a meeting (name/substring), filter by that substring in filenames.
- If no meeting specified: process everything in `tmp/` that you can confidently group per meeting.

3) Validate transcripts (per meeting):

- A meeting MUST have a Voice Recognition transcript.
- Detect VoiceRec by content header `TEXT| TIME| SPEAKER` (do not rely only on filename).
- If VoiceRec is missing or ambiguous: stop and ask the user what to do.
- Kontur Talk transcript is optional; if present, use it to improve speaker names; if absent, keep `SPEAKER_XX` and mention the limitation.

4) Collect unclear terms/typos (do not guess):

- Capture `term:proposed_fix` + 1 exact quote (1–2 lines) + 1 short note “о чём речь”.

5) Generate artifacts (per meeting):

- Create: `notes/meetings/artifacts/YYYY-MM-DD-<slug>/`
- Preserve raw exports, but add at the top: H1 + date + source/recording link (when available).
- Write:
  - `kontur-tolk-transcription.md` (raw)
  - `voice-rec-transcription-raw.md` (raw)
  - `voice-rec-transcription.md` (sorted by start time; derived from raw)
- Then move/remove the corresponding source files from `tmp/` so `tmp/` stays temporary.

6) Generate meeting note (per meeting):

- Create: `notes/meetings/YYYY-MM-DD-<slug>.md`
- Include links to the artifacts directory and each artifact file.
- Include a copy-ready protocol block (Russian) either at the top or bottom, with this exact structure:

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

- Before adding a typo, check if it already exists (limit output):

```bash
.venv/bin/python scripts/kb.py typos --query "TYPO" | tail -10
```

- Add newly confirmed typos/fixes using:

```bash
.venv/bin/python scripts/kb.py typos --add "TYPO" "FIX"
```

8) Index high-signal outcomes in MCP memory (optional, if allowed):

- Apply `kb-memory` policy (safety gate + “don’t duplicate KB”).
- Ensure a `Note:<path>` entity exists (entityType `Note`): `mcp__server-memory__search_nodes`, then `mcp__server-memory__create_entities` if missing.
- Add a single `pointer:` observation via `mcp__server-memory__add_observations`: 1–2 lines of what was decided + the note path.
- Optionally link to `Project:<...>` / `Person:<...>` via `mcp__server-memory__create_relations` if participants/projects are clear.
- Keep it minimal; never store raw transcript content in MCP memory.

9) Output to the user:

- By default, do not paste the entire protocol if it is already in the meeting note.
- Print created meeting note path(s): `notes/meetings/YYYY-MM-DD-<slug>.md`.
- Print unclear terms/typos with context for confirmation.
