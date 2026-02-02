---
name: codex-access-escalation
description: Escalate permissions/network access when Codex cannot proceed due to sandbox/approval/network restrictions or network/DNS failures (e.g., Jira sync, git push/pull, installs, curl/web, systemd, or writes outside allowed roots). Use when the request came via Telegram bot context (TELEGRAM_BOT_CONTEXT) and you must ask the user to resend the same command prefixed with `∆` (dangerous override, copy-ready). If the request is not from Telegram, instruct the user which Codex CLI flags to use (`--dangerously-bypass-approvals-and-sandbox`, `--sandbox danger-full-access`) and to restart with `codex resume` to preserve context.
---

# Codex Access Escalation

## Goal

Unblock tasks that require elevated execution (network, git push, systemd, installs, full disk access) by telling the user exactly how to re-run with the right access, without losing session context.

## Decision Flow

1) Detect that you’re blocked

You are likely blocked if any of these is true:

- Current run is strict (e.g. `approval_policy=never`, `network_access=restricted`, strict sandbox).
- The task needs forbidden capabilities (git push/pull, package installs, curl/web, systemctl, writing outside allowed roots).
- You hit network/DNS errors (e.g. `Temporary failure in name resolution`, `Name or service not known`) while doing `jira-sync`, `curl`, `git push`, etc. — these often mean the run needs network escalation.

2) Decide whether the user message came from Telegram

Treat as Telegram if the prompt includes a `TELEGRAM_BOT_CONTEXT` block or clearly states it’s coming from the Telegram bot.

3) If Telegram: ask for a `∆` re-send (copy-ready)

- Ask the user to re-send the same command prefixed with `∆`.
- Put the exact re-send text in a fenced code block so Telegram shows a copy button.
- Include only the user’s command/request (not the whole Telegram wrapper/context).
- Stop and wait for the `∆ …` message; do not “partially proceed”.

Template (replace `<USER_MESSAGE>` with the user’s intended command text):

```text
∆ <USER_MESSAGE>
```

Optional (Telegram UX): add a trailing tg_bot control block to show inline **Да/Нет** buttons instead of relying on heuristics:

```tg_bot
{"dangerous_confirm": true}
```

4) If NOT Telegram: tell the user how to restart Codex with full access + resume

Interactive (recommended):

- `codex resume --last --dangerously-bypass-approvals-and-sandbox --sandbox danger-full-access`
- If needed: add `-C <repo_root>` to resume in the same workspace.

Non-interactive (if they run via `codex exec`):

- `codex exec --dangerously-bypass-approvals-and-sandbox --sandbox danger-full-access -C <repo_root> resume --last -`

If they have a specific session id (UUID), use it instead of `--last`.

## Example Response (Telegram)

Нужен elevated доступ (сеть/пуш). Повтори команду с префиксом `∆` — просто скопируй блок и отправь:

```text
∆ <USER_MESSAGE>
```

```tg_bot
{"dangerous_confirm": true}
```
