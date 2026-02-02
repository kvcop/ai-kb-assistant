import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tg_bot import keyboards
from tg_bot.router import Router
from tg_bot.state import BotState


class _FakeAPI:
    def __init__(self) -> None:
        self.sends: list[dict[str, Any]] = []
        self.edits: list[dict[str, Any]] = []
        self.reply_markup_edits: list[dict[str, Any]] = []
        self.chunks: list[dict[str, Any]] = []
        self._next_message_id = 100

    def answer_callback_query(self, *, callback_query_id: str, text: str | None = None) -> None:
        return None

    def send_message(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
        timeout: int | None = None,
        **__: Any,
    ) -> dict[str, Any]:
        mid = self._next_message_id
        self._next_message_id += 1
        self.sends.append(
            {
                'chat_id': int(chat_id),
                'message_id': int(mid),
                'text': str(text),
                'reply_markup': reply_markup,
                'reply_to_message_id': int(reply_to_message_id or 0),
                'parse_mode': parse_mode,
                'timeout': int(timeout or 0),
            }
        )
        return {'ok': True, 'result': {'message_id': mid}}

    def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        self.edits.append(
            {
                'chat_id': int(chat_id),
                'message_id': int(message_id),
                'text': str(text),
                'parse_mode': parse_mode,
                'reply_markup': reply_markup,
            }
        )

    def edit_message_reply_markup(
        self, *, chat_id: int, message_id: int, reply_markup: dict[str, Any] | None = None
    ) -> None:
        self.reply_markup_edits.append(
            {'chat_id': int(chat_id), 'message_id': int(message_id), 'reply_markup': reply_markup}
        )

    def send_chunks(
        self,
        *,
        chat_id: int,
        text: str,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        reply_to_message_id: int | None = None,
    ) -> None:
        self.chunks.append(
            {
                'chat_id': int(chat_id),
                'text': str(text),
                'parse_mode': parse_mode,
                'reply_markup': reply_markup,
                'reply_to_message_id': int(reply_to_message_id or 0),
            }
        )


class _FakeWorkspaces:
    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root

    class _Paths:
        def __init__(self, repo_root: Path) -> None:
            self.repo_root = repo_root

    def ensure_workspace(self, chat_id: int) -> Any:
        return self._Paths(self._repo_root)


class _FakeCodex:
    def __init__(self, *, answer: str) -> None:
        self._answer = str(answer)

    def run_followup_by_profile_name(self, **_: Any) -> str:
        return self._answer


class _Profile:
    def __init__(self, *, name: str, sandbox: str | None, full_auto: bool) -> None:
        self.name = str(name)
        self.sandbox = sandbox
        self.full_auto = bool(full_auto)


class _FakeCodexRunner:
    def __init__(self, *, answer: str) -> None:
        self._answer = str(answer)
        self.chat_profile = _Profile(name='chat', sandbox='read-only', full_auto=False)
        self.auto_profile = _Profile(name='auto', sandbox=None, full_auto=True)
        self.danger_profile = _Profile(name='danger', sandbox='danger-full-access', full_auto=False)

    def log_note(self, *_: Any, **__: Any) -> None:
        return None

    def run_with_progress(self, *_: Any, **__: Any) -> str:
        return self._answer

    def run_dangerous_with_progress(self, *_: Any, **__: Any) -> str:
        return self._answer


def _mk_router(*, api: _FakeAPI, state: BotState, codex: _FakeCodex, repo_root: Path) -> Router:
    return Router(
        api=api,  # type: ignore[arg-type]
        state=state,
        codex=codex,  # type: ignore[arg-type]
        watcher=object(),  # type: ignore[arg-type]
        workspaces=_FakeWorkspaces(repo_root),  # type: ignore[arg-type]
        owner_chat_id=1,
        router_mode='heuristic',
        min_profile='read',
        force_write_prefix='∆',
        force_read_prefix='·',
        force_danger_prefix='!!',
        confidence_threshold=0.5,
        debug=False,
        dangerous_auto=True,
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
        runtime_queue_snapshot=None,
        runtime_queue_drop=None,
        runtime_queue_mutate=None,
        runtime_queue_edit_active=None,
        runtime_queue_edit_set=None,
    )


class TestRouterCallbackEditDelivery(unittest.TestCase):
    def test_followup_edits_progress_message_when_delivery_edit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=True)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, codex=_FakeCodex(answer='OK'), repo_root=Path(td))

            router.handle_callback(
                chat_id=1, user_id=1, data=keyboards.CB_CX_STATUS1, callback_query_id='cb', message_id=10
            )

            self.assertEqual(len(api.sends), 1)
            self.assertEqual(api.sends[0]['reply_to_message_id'], 10)
            self.assertEqual(api.chunks, [])

            self.assertTrue(api.edits)
            last = api.edits[-1]
            self.assertEqual(last['message_id'], api.sends[0]['message_id'])
            self.assertEqual(last['parse_mode'], 'HTML')
            self.assertTrue(str(last['text']).startswith('<b>'))
            self.assertIn('Статус 1 строка', str(last['text']))
            self.assertIn('OK', str(last['text']))
            self.assertIsInstance(last['reply_markup'], dict)

    def test_followup_reuses_ack_message_id_when_provided(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=True)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, codex=_FakeCodex(answer='PLAN'), repo_root=Path(td))

            router.handle_callback(
                chat_id=1,
                user_id=1,
                data=keyboards.CB_CX_PLAN3,
                callback_query_id='cb',
                message_id=10,
                ack_message_id=55,
            )

            self.assertEqual(api.sends, [])
            self.assertTrue(api.edits)
            last = api.edits[-1]
            self.assertEqual(last['message_id'], 55)
            self.assertEqual(last['parse_mode'], 'HTML')
            self.assertIn('План 3 шага', str(last['text']))
            self.assertIn('PLAN', str(last['text']))
            self.assertIsInstance(last['reply_markup'], dict)

    def test_dangerous_confirm_yes_edits_message_when_delivery_edit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=True)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            rid = 'abc123'
            st.set_pending_dangerous_confirmation(
                chat_id=1,
                request_id=rid,
                job={
                    'payload': 'do stuff',
                    'user_id': 1,
                    'message_id': 555,
                    'sent_ts': 0.0,
                    'created_ts': 0.0,
                    'expires_ts': 10**12,
                },
                max_per_chat=1,
            )

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, codex=_FakeCodexRunner(answer='OK'), repo_root=Path(td))  # type: ignore[arg-type]

            router.handle_callback(
                chat_id=1,
                user_id=1,
                data=f'{keyboards.CB_DANGER_ALLOW_PREFIX}{rid}',
                callback_query_id='cb',
                message_id=777,
            )

            self.assertEqual(api.sends, [])
            self.assertTrue(any(int(e.get('message_id') or 0) == 777 for e in api.edits))
            self.assertTrue(any('OK' in str(e.get('text') or '') for e in api.edits))

    def test_dangerous_confirm_no_edits_message_when_delivery_edit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=True)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            rid = 'def456'
            st.set_pending_dangerous_confirmation(
                chat_id=1,
                request_id=rid,
                job={
                    'payload': 'git push origin main',
                    'user_id': 1,
                    'message_id': 555,
                    'sent_ts': 0.0,
                    'created_ts': 0.0,
                    'expires_ts': 10**12,
                },
                max_per_chat=1,
            )

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, codex=_FakeCodexRunner(answer='OK'), repo_root=Path(td))  # type: ignore[arg-type]

            router.handle_callback(
                chat_id=1,
                user_id=1,
                data=f'{keyboards.CB_DANGER_DENY_PREFIX}{rid}',
                callback_query_id='cb',
                message_id=777,
            )

            self.assertEqual(api.sends, [])
            self.assertTrue(any(int(e.get('message_id') or 0) == 777 for e in api.edits))
            self.assertTrue(any('OK' in str(e.get('text') or '') for e in api.edits))
