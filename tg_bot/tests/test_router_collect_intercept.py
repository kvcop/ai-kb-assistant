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
        self.edited: list[dict[str, Any]] = []
        self._next_message_id = 1000

    def send_chat_action(self, *_: object, **__: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def send_message(self, **kwargs: object) -> dict[str, Any]:
        self.sent.append(dict(kwargs))
        mid = int(self._next_message_id)
        self._next_message_id += 1
        return {'ok': True, 'result': {'message_id': mid}}

    def send_chunks(self, **kwargs: object) -> None:
        self.sent.append(dict(kwargs))

    def edit_message_text(self, **kwargs: object) -> dict[str, Any]:
        self.edited.append(dict(kwargs))
        return {'ok': True, 'result': True}

    def delete_message(self, *_: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}


class _Profile:
    def __init__(self, *, name: str, sandbox: str | None, full_auto: bool) -> None:
        self.name = name
        self.sandbox = sandbox
        self.full_auto = bool(full_auto)


class _FakeCodexRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.chat_profile = _Profile(name='chat', sandbox='read-only', full_auto=False)
        self.auto_profile = _Profile(name='auto', sandbox=None, full_auto=True)
        self.danger_profile = _Profile(name='danger', sandbox='danger-full-access', full_auto=False)

    def log_note(self, *_: object, **__: object) -> None:
        return None

    def run_with_progress(self, *, prompt: str, automation: bool, chat_id: int, **__: Any) -> str:
        self.calls.append(('run_with_progress', {'automation': bool(automation), 'chat_id': int(chat_id), 'prompt': str(prompt)}))
        return 'OK'

    def run_dangerous_with_progress(self, *, prompt: str, chat_id: int, **__: Any) -> str:
        self.calls.append(('run_dangerous_with_progress', {'chat_id': int(chat_id), 'prompt': str(prompt)}))
        return 'OK'


def _mk_router(*, api: _FakeAPI, state: BotState, codex: _FakeCodexRunner, root: Path) -> Router:
    workspaces = WorkspaceManager(
        main_repo_root=root,
        owner_chat_id=1,
        workspaces_dir=root / 'workspaces',
        owner_uploads_dir=root / 'tg_uploads',
    )
    return Router(
        api=api,  # type: ignore[arg-type]
        state=state,
        codex=codex,  # type: ignore[arg-type]
        watcher=object(),  # type: ignore[arg-type]
        workspaces=workspaces,  # type: ignore[arg-type]
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


class TestRouterCollectIntercept(unittest.TestCase):
    def test_collect_active_intercepts_and_appends_text_with_attachments(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.append(chat_id=1, message_thread_id=7, item={'id': 'seed'})
            active = st.start(chat_id=1, message_thread_id=7)
            self.assertEqual(active, {'id': 'seed'})
            self.assertEqual(st.collect_status(chat_id=1, message_thread_id=7), 'active')

            api = _FakeAPI()
            codex = _FakeCodexRunner()
            router = _mk_router(api=api, state=st, codex=codex, root=root)

            payload_text = 'hello collect'
            attachments = [{'type': 'doc', 'id': 'a'}]
            reply_to = {'message_id': 555}
            received_ts = 1700000000.0

            router.handle_text(
                chat_id=1,
                message_thread_id=7,
                user_id=42,
                text=payload_text,
                attachments=attachments,
                reply_to=reply_to,
                message_id=333,
                received_ts=received_ts,
                ack_message_id=777,
            )

            self.assertEqual(codex.calls, [])
            pending = st.collect_pending.get('1:7')
            self.assertIsInstance(pending, list)
            self.assertTrue(pending)
            appended = pending[-1]
            self.assertEqual(appended.get('text'), payload_text)
            self.assertEqual(appended.get('message_id'), 333)
            self.assertEqual(appended.get('user_id'), 42)
            self.assertEqual(appended.get('received_ts'), received_ts)
            self.assertEqual(appended.get('attachments'), attachments)
            self.assertEqual(appended.get('reply_to'), reply_to)
            self.assertTrue(any('collect' in str(x.get('text', '')).lower() for x in api.edited))

    def test_collect_idle_calls_codex_run_with_progress(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            self.assertEqual(st.collect_status(chat_id=1, message_thread_id=7), 'idle')

            api = _FakeAPI()
            codex = _FakeCodexRunner()
            router = _mk_router(api=api, state=st, codex=codex, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='hello router', message_id=333)

            self.assertTrue(any(name == 'run_with_progress' for name, _ in codex.calls))
