import os
import re
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from tg_bot.router import Router
from tg_bot.state import BotState
from tg_bot.workspaces import WorkspaceManager


class _FakeAPI:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.docs: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []

    def send_message(self, **kwargs: object) -> dict[str, object]:
        self.sent.append(dict(kwargs))  # type: ignore[arg-type]
        return {'ok': True, 'result': {'message_id': 1}}

    def edit_message_text(self, **_: object) -> dict[str, object]:
        return {'ok': True, 'result': {'message_id': 1}}

    def edit_message_reply_markup(self, **_: object) -> dict[str, object]:
        return {'ok': True, 'result': {'message_id': 1}}

    def send_document(self, **kwargs: object) -> dict[str, object]:
        self.docs.append(dict(kwargs))  # type: ignore[arg-type]
        return {'ok': True, 'result': {'message_id': 2}}

    def delete_message(self, **kwargs: object) -> dict[str, object]:
        self.deleted.append(dict(kwargs))  # type: ignore[arg-type]
        return {'ok': True, 'result': True}


def _mk_router(*, api: _FakeAPI, state: BotState, workspaces: WorkspaceManager) -> Router:
    return Router(
        api=api,  # type: ignore[arg-type]
        state=state,
        codex=object(),  # type: ignore[arg-type]
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
        history_max_events=10,
        history_context_limit=10,
        history_entry_max_chars=400,
        codex_followup_sandbox='read-only',
    )


class TestRouterUploadCommand(unittest.TestCase):
    def test_upload_sends_file_as_document(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'tg_uploads').mkdir(parents=True, exist_ok=True)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            p = root / 'hello.txt'
            p.write_text('hi', encoding='utf-8')

            workspaces = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=1,
                workspaces_dir=root / 'workspaces',
                owner_uploads_dir=root / 'tg_uploads',
            )
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, workspaces=workspaces)

            router.handle_text(chat_id=1, message_thread_id=123, user_id=1, text='/upload hello.txt', message_id=100)

            deadline = time.time() + 1.0
            while len(api.docs) < 1 and time.time() < deadline:
                time.sleep(0.01)

            self.assertEqual(len(api.docs), 1)
            self.assertEqual(len(api.sent), 1)
            self.assertTrue(str(api.docs[0].get('document_path') or '').endswith('hello.txt'))
            self.assertEqual(int(api.docs[0].get('message_thread_id') or 0), 123)
            self.assertEqual(int(api.docs[0].get('reply_to_message_id') or 0), 100)
            self.assertIn('hello.txt', str(api.docs[0].get('caption') or ''))
            deadline = time.time() + 1.0
            while len(api.deleted) < 1 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(len(api.deleted), 1)
            self.assertEqual(int(api.deleted[0].get('chat_id') or 0), 1)
            self.assertEqual(int(api.deleted[0].get('message_id') or 0), 1)

    def test_upload_command_works_with_router_force_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'tg_uploads').mkdir(parents=True, exist_ok=True)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            p = root / 'hello.txt'
            p.write_text('hi', encoding='utf-8')

            workspaces = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=1,
                workspaces_dir=root / 'workspaces',
                owner_uploads_dir=root / 'tg_uploads',
            )
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, workspaces=workspaces)

            router.handle_text(chat_id=1, message_thread_id=1, user_id=1, text='∆/upload hello.txt', message_id=100)
            router.handle_text(chat_id=1, message_thread_id=2, user_id=1, text='∆ /upload hello.txt', message_id=101)

            deadline = time.time() + 1.0
            while len(api.docs) < 2 and time.time() < deadline:
                time.sleep(0.01)

            self.assertEqual(len(api.docs), 2)
            self.assertEqual(len(api.sent), 2)
            self.assertEqual(int(api.docs[0].get('message_thread_id') or 0), 1)
            self.assertEqual(int(api.docs[1].get('message_thread_id') or 0), 2)
            self.assertEqual(int(api.docs[0].get('reply_to_message_id') or 0), 100)
            self.assertEqual(int(api.docs[1].get('reply_to_message_id') or 0), 101)
            deadline = time.time() + 1.0
            while len(api.deleted) < 2 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(len(api.deleted), 2)

    def test_upload_zips_file_when_forced(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'tg_uploads').mkdir(parents=True, exist_ok=True)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            p = root / 'hello.txt'
            p.write_text('hi', encoding='utf-8')

            workspaces = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=1,
                workspaces_dir=root / 'workspaces',
                owner_uploads_dir=root / 'tg_uploads',
            )
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, workspaces=workspaces)

            old = os.environ.get('TG_SEND_MAX_MB')
            os.environ['TG_SEND_MAX_MB'] = '50'
            try:
                router.handle_text(
                    chat_id=1, message_thread_id=456, user_id=1, text='/upload --zip hello.txt', message_id=100
                )
            finally:
                if old is None:
                    os.environ.pop('TG_SEND_MAX_MB', None)
                else:
                    os.environ['TG_SEND_MAX_MB'] = old

            deadline = time.time() + 1.0
            while len(api.docs) < 1 and time.time() < deadline:
                time.sleep(0.01)

            self.assertEqual(len(api.docs), 1)
            self.assertEqual(len(api.sent), 1)
            doc_path = str(api.docs[0].get('document_path') or '')
            self.assertEqual(int(api.docs[0].get('message_thread_id') or 0), 456)
            self.assertTrue(doc_path.endswith('.zip'))
            self.assertTrue(Path(doc_path).exists(), msg=f'expected zip to exist: {doc_path}')
            deadline = time.time() + 1.0
            while len(api.deleted) < 1 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(len(api.deleted), 1)
