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
    def __init__(self, *, reminders_file: Path) -> None:
        self.reminders_file = reminders_file
        self.reminders_include_weekends = True


class TestRouterRemindersCommand(unittest.TestCase):
    def _make_router(self, *, root: Path, st: BotState, api: _FakeAPI, watcher: _FakeWatcher) -> Router:
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
            watcher=watcher,  # type: ignore[arg-type]
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

    def test_reminders_sets_target_and_lists_today(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders_path = root / 'notes' / 'work' / 'reminders.md'
            reminders_path.parent.mkdir(parents=True, exist_ok=True)
            reminders_path.write_text('range:2000-01-01..3000-01-01@10:00\tHello\n', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            watcher = _FakeWatcher(reminders_file=reminders_path)
            router = self._make_router(root=root, st=st, api=api, watcher=watcher)

            router.handle_text(chat_id=-100, message_thread_id=777, user_id=1, text='/reminders', message_id=123)

            self.assertEqual(st.reminders_target(), (-100, 777))
            self.assertEqual(len(api.sent_messages), 1)

            sent = api.sent_messages[0]
            self.assertEqual(int(sent.get('chat_id') or 0), -100)
            self.assertEqual(int(sent.get('message_thread_id') or 0), 777)
            text = str(sent.get('text') or '')
            self.assertIn('✅', text)
            self.assertIn('Hello', text)
