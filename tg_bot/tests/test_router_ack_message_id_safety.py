import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tg_bot.router import Router
from tg_bot.state import BotState
from tg_bot.workspaces import WorkspaceManager


class _FakeAPI:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.sent_message_ids: list[int] = []
        self.edited: list[dict[str, Any]] = []
        self._next_message_id = 1000

    def send_chat_action(self, *_: object, **__: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def send_message(self, **kwargs: object) -> dict[str, Any]:
        self.sent.append(dict(kwargs))
        mid = int(self._next_message_id)
        self._next_message_id += 1
        self.sent_message_ids.append(mid)
        return {'ok': True, 'result': {'message_id': mid}}

    def send_chunks(self, **kwargs: object) -> None:
        self.sent.append(dict(kwargs))

    def edit_message_text(self, **kwargs: object) -> dict[str, Any]:
        self.edited.append(dict(kwargs))
        return {'ok': True, 'result': True}

    def delete_message(self, **_: object) -> dict[str, Any]:
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

    def run_dangerous_with_progress(self, *_: object, **__: object) -> str:
        return 'OK'


class TestRouterAckMessageIdSafety(unittest.TestCase):
    def _make_router(self, *, root: Path, api: _FakeAPI, st: BotState) -> Router:
        workspaces = WorkspaceManager(
            main_repo_root=root,
            owner_chat_id=1,
            workspaces_dir=root / 'workspaces',
            owner_uploads_dir=root / 'tg_uploads',
        )
        return Router(
            api=api,  # type: ignore[arg-type]
            state=st,
            codex=_FakeCodexRunner(),  # type: ignore[arg-type]
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
            tg_progress_edit_enabled=False,
            tg_progress_edit_interval_seconds=10,
            tg_codex_parse_mode='HTML',
            fallback_patterns=re.compile(r'$^'),
            gentle_default_minutes=60,
            gentle_auto_mute_window_minutes=60,
            gentle_auto_mute_count=3,
            history_max_events=50,
            history_context_limit=10,
            history_entry_max_chars=400,
            codex_followup_sandbox='read-only',
        )

    def test_prefers_ack_mapping_over_event_ack_message_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            # Expected ack for incoming message_id=100.
            st.tg_bind_message_id_for_coalesce_key(chat_id=1, coalesce_key='ack:1:100', message_id=123)
            # Simulate a stale ack id from some other message.
            st.tg_bind_message_id_for_coalesce_key(chat_id=1, coalesce_key='ack:1:50', message_id=999)

            api = _FakeAPI()
            router = self._make_router(root=root, api=api, st=st)

            router.handle_text(chat_id=1, user_id=1, text='∆ hello', message_id=100, ack_message_id=999)

            edited_ids = [int(x.get('message_id') or 0) for x in api.edited]
            self.assertIn(123, edited_ids)
            self.assertNotIn(999, edited_ids)

    def test_ignores_stale_event_ack_message_id_when_mapping_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            # No mapping for ack:1:100, but the provided ack_message_id=999 is known to belong to a different key.
            st.tg_bind_message_id_for_coalesce_key(chat_id=1, coalesce_key='ack:1:50', message_id=999)

            api = _FakeAPI()
            router = self._make_router(root=root, api=api, st=st)

            router.handle_text(chat_id=1, user_id=1, text='∆ hello', message_id=100, ack_message_id=999)

            self.assertTrue(api.sent_message_ids)
            progress_mid = api.sent_message_ids[0]
            edited_ids = [int(x.get('message_id') or 0) for x in api.edited]
            self.assertIn(progress_mid, edited_ids)
            self.assertNotIn(999, edited_ids)

    def test_status_shows_per_topic_resume_for_thread_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            api = _FakeAPI()
            router = self._make_router(root=root, api=api, st=st)

            router.handle_text(chat_id=1, message_thread_id=777, user_id=1, text='∆ hello', message_id=100)

            texts = [str(x.get('text') or '') for x in api.edited]
            self.assertTrue(any('per-topic resume' in t for t in texts), texts)
