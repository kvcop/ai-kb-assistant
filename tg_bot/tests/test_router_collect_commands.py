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

    def send_chat_action(self, *_: object, **__: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def send_message(self, **kwargs: object) -> dict[str, Any]:
        self.sent.append(dict(kwargs))  # type: ignore[arg-type]
        return {'ok': True, 'result': {'message_id': 1}}

    def edit_message_text(self, **_: object) -> dict[str, Any]:
        return {'ok': True, 'result': {'message_id': 1}}

    def delete_message(self, *_: object) -> dict[str, Any]:
        return {'ok': True, 'result': True}


def _mk_router(*, api: _FakeAPI, state: BotState, root: Path) -> Router:
    workspaces = WorkspaceManager(
        main_repo_root=root,
        owner_chat_id=1,
        workspaces_dir=root / 'workspaces',
        owner_uploads_dir=root / 'tg_uploads',
    )
    return Router(
        api=api,  # type: ignore[arg-type]
        state=state,
        codex=object(),  # type: ignore[arg-type]
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


def _last_text(api: _FakeAPI) -> str:
    return str(api.sent[-1].get('text', ''))


class TestRouterCollectCommands(unittest.TestCase):
    def test_collect_status_empty_and_start_done_retry_cancel_negative_states(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect status', message_id=100)
            self.assertIn('state: idle', _last_text(api))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect start', message_id=101)
            self.assertIn('collect start: очередь пуста', _last_text(api))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect done', message_id=102)
            self.assertIn('collect done: нет активного', _last_text(api))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect cancel', message_id=103)
            self.assertIn('collect cancel: нет активного', _last_text(api))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect retry', message_id=104)
            self.assertIn('collect retry: нет deferred item', _last_text(api))

    def test_collect_start_done_and_retry_lifecycle_with_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            st.append(chat_id=1, message_thread_id=7, item={'id': 'task-1'})

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect start', message_id=200)
            self.assertIn('collect start: активен item task-1', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect done', message_id=201)
            self.assertIn('collect done: active item task-1 завершён', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'deferred')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect retry', message_id=202)
            self.assertIn('collect retry: deferred item task-1 активирован', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')

    def test_collect_commands_are_isolated_by_message_thread(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            st.append(chat_id=1, message_thread_id=0, item={'id': 'root-item'})
            st.append(chat_id=1, message_thread_id=7, item={'id': 'thread-item'})

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect start', message_id=300)
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')
            self.assertEqual(st.status(chat_id=1, message_thread_id=0), 'pending')

            router.handle_text(chat_id=1, message_thread_id=0, user_id=42, text='/collect status', message_id=301)
            self.assertIn('state: pending', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=0), 'pending')
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect cancel', message_id=302)
            self.assertIn('collect cancel: active item thread-item отменён', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'idle')

            router.handle_text(chat_id=1, message_thread_id=0, user_id=42, text='/collect status', message_id=303)
            self.assertIn('state: pending', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=0), 'pending')
