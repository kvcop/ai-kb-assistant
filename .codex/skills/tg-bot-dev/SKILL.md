---
name: tg-bot-dev
description: 'Develop and maintain `tg_bot/` (Telegram bot) in this repo: keep changes modular (KISS/DRY), distinguish control-plane vs work-plane (queue/Codex), add tests while implementing, do lazy refactors, and commit+push frequently.'
---

# tg-bot-dev

Use this skill when you need to change the Telegram bot under `tg_bot/` (commands, queue/control plane, keyboards, state, delivery, systemd behaviour).

## Key principles

- **Control plane vs work plane**: “control plane” actions must work even while Codex is busy; Codex-triggering work stays serialized via the queue.
- **KISS / modularity**: keep `router.py` focused on semantics, `keyboards.py` on callback_data + markup, `app.py` on runtime wiring (poll/worker), `state.py` on persistence.
- **Tests-first during implementation**: for every new callback/command/state field, add or extend unit tests under `tg_bot/tests/` while you code.
- **Lazy refactor**: if you touch a function/file that’s getting too complex and the refactor is small, do it immediately. If it’s non-trivial/risky, write a refactor plan as a separate tech note under `notes/technical/` (what/why/how, scope boundaries).
- **Callback data hygiene**: `callback_data` must stay `<= 64 bytes`; keep prefixes short and add tests for encoding/limits.

## Workflow

1) **Orient on current behaviour**
- Read `tg_bot/README.md` for the contract (commands, env vars, safety rules).
- For queue-related changes, locate existing plan/notes in `notes/technical/`.

2) **Implement in the right layer**
- `tg_bot/router.py`: parse/route commands and callbacks; keep behaviour deterministic.
- `tg_bot/keyboards.py`: define `CB_*` constants and keyboard builders (no business logic).
- `tg_bot/app.py`: poller/worker wiring, queue/spool, bypass rules (control plane).
- `tg_bot/state.py`: persistent state only (add load/save + defaults + tests).

3) **Add tests while coding**
- Prefer small unit tests (no network; no Telegram calls) under `tg_bot/tests/`.
- Use the existing `python-tests` skill for exact commands and TMPDIR handling when running tests.

4) **Validate quickly**
- Syntax smoke: `python3 -m py_compile tg_bot/app.py tg_bot/router.py tg_bot/keyboards.py tg_bot/state.py`
- Unit tests: `TMPDIR="$PWD/.tmp/pytests" python3 -m unittest discover -s tg_bot/tests -v`

5) **Commit and push frequently**
- Keep commits small and conventional (`feat(tg-bot): ...`, `fix(tg-bot): ...`, `test(tg-bot): ...`).
- Push after each logically complete slice (so deploy/restart is easy to validate).

