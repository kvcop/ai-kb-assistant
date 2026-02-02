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
        self.sent_messages: list[dict[str, Any]] = []

    def send_chat_action(self, *_: object, **__: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def send_message(self, **kwargs: object) -> dict[str, Any]:
        self.sent_messages.append(dict(kwargs))
        return {'ok': True, 'result': {'message_id': 1}}

    def edit_message_text(self, **_: object) -> dict[str, Any]:
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


class _FakeWatcher:
    def __init__(self) -> None:
        self.reminders_file = Path('.')
        self.reminders_include_weekends = True


class TestRouterMmOtpCommand(unittest.TestCase):
    def _make_router(self, *, root: Path, st: BotState, api: _FakeAPI) -> Router:
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
            watcher=_FakeWatcher(),  # type: ignore[arg-type]
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

    def test_mm_otp_sets_token_in_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = self._make_router(root=root, st=st, api=api)

            router.handle_text(chat_id=-100, message_thread_id=777, user_id=1, text='/mm-otp 123456', message_id=10)

            self.assertEqual(st.mm_consume_mfa_token(max_age_seconds=999), '123456')
            self.assertEqual(len(api.sent_messages), 1)
            sent = api.sent_messages[0]
            self.assertEqual(int(sent.get('chat_id') or 0), -100)
            self.assertEqual(int(sent.get('message_thread_id') or 0), 777)
            self.assertIn('MFA', str(sent.get('text') or ''))

    def test_mm_reset_clears_mattermost_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.mm_mark_sent(channel_id='chan-a', up_to_ts=111)
            st.mm_mark_pending(channel_id='chan-b', up_to_ts=222)
            st.mm_set_mfa_token('123456')
            st.mm_mark_mfa_required()
            st.mm_set_session_token('sess-123')

            api = _FakeAPI()
            router = self._make_router(root=root, st=st, api=api)
            router.handle_text(chat_id=-100, message_thread_id=777, user_id=1, text='/mm-reset', message_id=10)

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.mm_sent_up_to_ts('chan-a'), 0)
            self.assertEqual(st2.mm_pending_up_to_ts('chan-b'), 0)
            self.assertEqual(st2.mm_consume_mfa_token(max_age_seconds=999), '')
            self.assertFalse(st2.mm_is_mfa_required())
            self.assertEqual(st2.mm_get_session_token(), '')
