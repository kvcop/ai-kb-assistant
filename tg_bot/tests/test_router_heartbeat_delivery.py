import re
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

from tg_bot.router import Router
from tg_bot.state import BotState
from tg_bot.workspaces import WorkspaceManager


class _FakeAPI:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.edited: list[dict[str, Any]] = []

    def send_chat_action(self, *_: object, **__: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def send_message(self, **kwargs: object) -> dict[str, Any]:
        self.sent.append(dict(kwargs))
        return {'ok': True, 'result': {'message_id': 1}}

    def send_chunks(self, **kwargs: object) -> None:
        self.sent.append(dict(kwargs))

    def edit_message_text(self, **kwargs: object) -> dict[str, Any]:
        self.edited.append(dict(kwargs))
        return {'ok': True, 'result': True}


class _Profile:
    def __init__(self, *, name: str, sandbox: str | None, full_auto: bool) -> None:
        self.name = name
        self.sandbox = sandbox
        self.full_auto = bool(full_auto)


class _FakeCodexRunner:
    def __init__(self) -> None:
        self.chat_profile = _Profile(name='chat', sandbox='read-only', full_auto=False)
        self.auto_profile = _Profile(name='auto', sandbox=None, full_auto=True)
        self.danger_profile = _Profile(name='danger', sandbox='danger-full-access', full_auto=False)

    def log_note(self, *_: object, **__: object) -> None:
        return None

    def run_with_progress(self, *_: object, **__: object) -> str:
        return 'OK'


class _StuckThread:
    def join(self, timeout: float | None = None) -> None:
        return None

    def is_alive(self) -> bool:
        return True


class _RouterStuckHeartbeat(Router):
    def _start_heartbeat(  # type: ignore[override]
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        ack_message_id: int,
        ack_coalesce_key: str = '',
        started_ts: float,
        status: dict[str, str],
    ) -> tuple[threading.Event, _StuckThread]:
        return (threading.Event(), _StuckThread())


class TestRouterHeartbeatDelivery(unittest.TestCase):
    def test_falls_back_to_send_when_heartbeat_cant_stop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=True)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            api = _FakeAPI()
            codex = _FakeCodexRunner()
            workspaces = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=1,
                workspaces_dir=root / 'workspaces',
                owner_uploads_dir=root / 'tg_uploads',
            )

            router = _RouterStuckHeartbeat(
                api=api,  # type: ignore[arg-type]
                state=st,
                codex=codex,  # type: ignore[arg-type]
                watcher=object(),  # type: ignore[arg-type]
                workspaces=workspaces,
                owner_chat_id=1,
                router_mode='heuristic',
                min_profile='read',
                force_write_prefix='!',
                force_read_prefix='?',
                force_danger_prefix='∆',
                confidence_threshold=0.5,
                debug=False,
                dangerous_auto=False,
                tg_typing_enabled=False,
                tg_typing_interval_seconds=10,
                tg_progress_edit_enabled=True,
                tg_progress_edit_interval_seconds=10,
                tg_codex_parse_mode='',
                fallback_patterns=re.compile(r'$^'),
                gentle_default_minutes=60,
                gentle_auto_mute_window_minutes=60,
                gentle_auto_mute_count=3,
                history_max_events=50,
                history_context_limit=10,
                history_entry_max_chars=400,
                codex_followup_sandbox='read-only',
            )

            router.handle_text(chat_id=1, user_id=1, text='? hello', message_id=100)

            self.assertTrue(any(str(x.get('text') or '') == 'OK' for x in api.sent))
            self.assertFalse(any(str(x.get('text') or '') == 'OK' for x in api.edited))
