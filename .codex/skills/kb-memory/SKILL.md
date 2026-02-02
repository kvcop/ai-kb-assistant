---
name: kb-memory
description: Persistent MCP memory policy for this KB (server-memory knowledge graph). Use for stable preferences, people/roles mappings, and high-signal pointers to KB notes. Triggered by "память", "запомни", "не забудь", "забудь", "remember", "forget".
---

# kb-memory

Use this skill as a **policy layer** for persistent memory via MCP `server-memory` (knowledge graph).

## Principles (keep KB canonical)

- `notes/work/**` is the source of truth for daily state (plan, progress, time tracking).
- MCP memory is for **stable, high-signal** information and **pointers** to KB files, not for duplicating markdown content.
- Prefer retrieval (`search_nodes` + `open_nodes`) over dumping the whole graph (`read_graph`).

## Safety gate (Telegram / multi-tenant)

If the injected chat context indicates any of the following:
- `kb_scope: isolated (per-chat)` (or any non-owner / shared workspace mode),
- group/non-owner context,

then **do not read or write MCP memory** (avoid leaking owner memory across chats).

## Recall loop (start of a user turn, when allowed)

1) Build a short query from the user message: project names, people, Jira keys, key nouns (3–8 tokens).
2) Run `mcp__server-memory__search_nodes` with that query.
3) Open only top 1–3 relevant nodes via `mcp__server-memory__open_nodes`.
4) Extract only a small set of observations (5–12 lines max) to inform the answer.

Avoid `mcp__server-memory__read_graph` except debugging.

## What is worth remembering (write triggers)

Write/update MCP memory only when:
- user explicitly says “запомни / не забудь / забудь / больше так не делай / всегда делай так”;
- a stable preference or durable personal fact is stated;
- a new person↔role mapping appears;
- a durable decision/agreement is made **and** there is (or will be) a KB artifact you can point to.

If the info is “today-only” (progress, time spent, daily plan) → keep it in `notes/work/**`, not in MCP memory.

## Naming conventions (avoid duplicates)

Use stable entity names:
- `User:<name>` (single main user entity)
- `Person:<Full Name>`
- `Project:<short>` (e.g., `Project:ESO`)
- `Note:<relative_path>` (e.g., `Note:notes/meetings/2025-12-28-eso-sync.md`)
- `Jira:<KEY>` (optional)
- `Decision:<slug>` (optional)

Recommended `entityType` values (match the prefix):
- `User:<name>` → `User`
- `Person:<Full Name>` → `Person`
- `Project:<short>` → `Project`
- `Note:<relative_path>` → `Note`
- `Jira:<KEY>` → `Jira`
- `Decision:<slug>` → `Decision`

Always `mcp__server-memory__search_nodes` first; only then `mcp__server-memory__create_entities`.

## Observation format (easy to update/delete)

Keep each observation as a single line (one fact), key-value style:
- `pref: answer_format=1_screen (as_of=YYYY-MM-DD, src=user)`
- `role: <...> (as_of=YYYY-MM-DD, src=kb|user|meeting)`
- `pointer: notes/meetings/...md — 1–2 line summary (as_of=YYYY-MM-DD, src=kb)`

When a fact changes:
- remove the exact old observation via `mcp__server-memory__delete_observations`
- add the new one via `mcp__server-memory__add_observations`

## Minimal graph patterns

Preferences live on `User:<name>`.

Pointers live on `Note:<path>` and can be linked:
- `Note:<path>` -[covers]-> `Project:<...>`
- `Note:<path>` -[mentions]-> `Person:<...>`
- `Note:<path>` -[references]-> `Jira:<KEY>` (optional)

Relation naming:
- keep `relationType` in `snake_case` (`reports_to`, `covers`, `mentions`, `references`)
- avoid synonyms (pick one verb and stick to it)

## KB indexing recipe (batch)

When asked to “index the KB into MCP memory”, do a **pointer index**, not a content dump:

- Include: `notes/**/*.md` + optionally `README.md`.
- Exclude (noise / raw / legacy): `archive/legacy/**`, `notes/meetings/artifacts/**`, `notes/daily-logs/**`, `notes/work/daily-brief.md`, `notes/work/end-of-day.md`.
- For each selected markdown file:
  - Create `Note:<relative_path>` (entityType `Note`) with a single `pointer:` observation using the file H1 title.
  - Extract Jira keys (project keys, e.g. `RND-123`, `RUMA-4480`) → create `Jira:<KEY>` entities (entityType `Jira`) and link via `Note:<path> -[references]-> Jira:<KEY>`.
  - For meeting notes: parse `**Participants**:` / `**Участники**:` and link `Note:<path> -[mentions]-> Person:<name>` (avoid duplicating the main user; reuse `User:<name>` when obvious).

## Tool payload templates (copy-ready)

Payload for `mcp__server-memory__create_entities`:
```json
{"entities":[{"name":"User:Example User","entityType":"User","observations":["pref: answer_format=1_screen (as_of=2025-12-28, src=user)"]}]}
```

Payload for `mcp__server-memory__add_observations`:
```json
{"observations":[{"entityName":"User:Example User","contents":["pref: answer_format=1_screen (as_of=2025-12-28, src=user)"]}]}
```

Payload for `mcp__server-memory__delete_observations` (exact string match):
```json
{"deletions":[{"entityName":"User:Example User","observations":["pref: answer_format=1_screen (as_of=2025-12-28, src=user)"]}]}
```

Payload for `mcp__server-memory__create_relations`:
```json
{"relations":[{"from":"Note:notes/meetings/2025-12-28-sync.md","relationType":"covers","to":"Project:Example"}]}
```

## “Forget” behavior

If user says “забудь X”:
- locate relevant nodes via `mcp__server-memory__search_nodes` + `mcp__server-memory__open_nodes`
- remove specific observation(s) / relation(s) (`mcp__server-memory__delete_observations` / `mcp__server-memory__delete_relations`)
- confirm in chat what was removed (short, explicit).

## If MCP memory is unavailable (setup gate)

If MCP tools `mcp__server-memory__*` are not available (server is not configured / `npx` is missing / MCP is disabled):

1) **Stop and ask the user for permission** to enable/install MCP `server-memory` (this may download code and may modify `~/.codex/config.toml`).
2) If the user agrees, set it up:
   - Add the server: `codex mcp add server-memory -- npx -y @modelcontextprotocol/server-memory`
   - Ensure MCP server passes `MEMORY_FILE_PATH` through (check `codex mcp get server-memory --json` → `"env_vars": ["MEMORY_FILE_PATH"]`).
   - Ensure `MEMORY_FILE_PATH` points to the intended storage (repo default: `.mcp/server-memory.jsonl`).
3) Verify by calling `mcp__server-memory__read_graph` (or any targeted tool like `search_nodes`).

If running via Telegram bot context and the execution policy requires an explicit “dangerous” confirmation for installs/config edits, ask the user to re-send the same request prefixed with `∆` (see `codex-access-escalation`).

## Never store

- secrets/tokens/passwords/keys
- raw transcripts / large blobs of text
- daily worklog details already present in `notes/work/**`
