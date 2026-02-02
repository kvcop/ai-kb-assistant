from __future__ import annotations


def codex_resume_label(*, message_thread_id: int | None) -> str:
    """User-facing label for Codex resume mode (chat vs topic scope)."""
    tid = int(message_thread_id or 0)
    if tid > 0:
        return 'per-topic resume'
    return 'per-chat resume'
