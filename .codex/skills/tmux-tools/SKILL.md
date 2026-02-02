---
name: tmux-tools
description: Remote-safe tmux monitoring and interaction: list panes, capture pane text, create snapshots (txt/png) into tg_uploads/, optionally send them to Telegram, and (rarely) send keys with an explicit dangerous flag.
---

# tmux-tools

Use this skill when the user asks to:
- get a tmux “screenshot” / current pane output,
- watch a long-running job from Telegram,
- copy back logs/text from a pane,
- (rarely) type into a pane / press Enter (explicitly requested).

Core tool: `python3 scripts/tmux_tools.py ...`

## Safety defaults

- Prefer read-only operations (`ls`, `tail`, `snap`).
- `send-keys` is **dangerous** and requires `--dangerous`. Do **not** send Ctrl+C / `exit` / kill commands unless the user explicitly asks.
- When unsure which pane to act on: run `ls` first and ask the user to pick a target.

## Quick start

List panes:
```bash
python3 scripts/tmux_tools.py ls
python3 scripts/tmux_tools.py ls --session 3
```

Get text from a pane (stdout):
```bash
python3 scripts/tmux_tools.py tail --target %29
python3 scripts/tmux_tools.py tail --target 3:4.1 --lines 200
```

Send captured text back to Telegram (message):
```bash
python3 scripts/tmux_tools.py tail --target %29 --lines 60 --send
```

Create a snapshot file under `tg_uploads/` (txt + optional png):
```bash
python3 scripts/tmux_tools.py snap --target %29
python3 scripts/tmux_tools.py snap --session 3
python3 scripts/tmux_tools.py snap --session 3 --lines 200 --no-png
```

Send the snapshot to Telegram (as a document):
```bash
python3 scripts/tmux_tools.py snap --session 3 --send --caption "tmux session 3"
```

If you already have an artifact file, return it via bot:
```text
/upload tg_uploads/<file>
```

## Target selection rules (when `--target` is omitted)

`tmux_tools.py` picks a default target in this order:
1) If running inside tmux (`$TMUX` is set) → current `#{pane_id}`
2) Else → pane attached to the most recently active tmux client
3) Else → the first pane from `tmux list-panes -a`

## Telegram sending (script mode)

`--send` uses Telegram Bot API directly.

Requirements:
- `TG_BOT_TOKEN` must be available (env or `.env.tg_bot`)
- `TG_OWNER_CHAT_ID` must be set (or pass `--chat-id`)
- Optional: `--thread-id` (topic/thread id)

## Dangerous mode: send keys

Only when explicitly requested:
```bash
python3 scripts/tmux_tools.py send-keys --dangerous --target %29 --text "status" --enter
python3 scripts/tmux_tools.py send-keys --dangerous --target %29 --dry-run --text "echo hi" --enter
```

