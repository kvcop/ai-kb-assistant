---
name: kb-log-ideas
description: Capture raw ideas (Telegram notes / Voice Recognition transcripts) into notes/ideas as draft idea notes with TL;DR, open questions, and raw transcript; keep open-questions uncluttered.
---

# kb-log-ideas

Use this skill when the user asks to **record an idea** ("идея", "запиши идею", "зафиксируй идею", "idea inbox"), especially from Telegram messages or Voice Recognition transcripts.

Goal: store **unprocessed ideas** in `notes/ideas/` (or `notes/personal/` if explicitly personal), in a consistent format that can later be promoted into `notes/technical/` or Jira.

## Workflow

1) Anchor time
- Get local time: `date '+%Y-%m-%d %H:%M (%A) %Z'` and use the date in filenames.

2) Choose scope (work vs personal)
- Default: work idea -> `notes/ideas/`.
- If the user explicitly says personal/weekend/pet-project -> `notes/personal/` (create `notes/personal/ideas/` if needed).

3) Create a new idea note
- Path: `notes/ideas/YYYY-MM-DD-<slug>.md` (or `notes/personal/ideas/...`).
- Slug rules:
  - Prefer short ASCII (`ralph-loop-orchestrator`, `mcp-git-push`).
  - If unclear, use `idea-<HHMM>` or `idea-<shortid>`.
- The note must start with an H1 and include a date line.

4) Use the standard structure (keep it raw)
Include:
- `Date` + `Source` (Telegram text / Voice Recognition transcript; include local file path if attachments exist).
- `TL;DR` (3-6 bullets max).
- Main sections (1-3) describing the idea at a high level.
- `Open Questions` (things to clarify later).
- `Raw transcript (ASR)` in a fenced code block (paste as-is; light typo fixes allowed only if meaning is preserved).

5) Keep `notes/work/open-questions.md` clean
- Do not add a TODO just because an idea exists.
- Add an entry only if the user requested a near-term follow-up (decision needed / task to do soon), and link to the idea note.

## Template (copy)

```md
# Идеи: <тема> (черновик)
**Date**: YYYY-MM-DD
**Source**: <Telegram text / Voice Recognition transcript>; attachments: `<path>` (if any)

## Коротко (TL;DR)
- ...

## Суть / проблема
- ...

## Идея / подход
- ...

## Открытые вопросы
- ...

## Raw transcript (ASR)
~~~text
<raw>
~~~
```
