import re
import tempfile
import threading
import time
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
        **__: Any,
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
        **__: Any,
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


class _Profile:
    def __init__(self, *, name: str, sandbox: str | None, full_auto: bool) -> None:
        self.name = str(name)
        self.sandbox = sandbox
        self.full_auto = bool(full_auto)


class _FakeCodexRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.chat_profile = _Profile(name='chat', sandbox='read-only', full_auto=False)
        self.auto_profile = _Profile(name='auto', sandbox=None, full_auto=True)
        self.danger_profile = _Profile(name='danger', sandbox='danger-full-access', full_auto=False)

    def log_note(self, *_: Any, **__: Any) -> None:
        return None

    def run_with_progress(self, *, prompt: str, automation: bool, chat_id: int, **__: Any) -> str:
        self.calls.append(
            ('run_with_progress', {'automation': bool(automation), 'chat_id': int(chat_id), 'prompt': str(prompt)})
        )
        return 'OK'

    def run_dangerous_with_progress(self, *, prompt: str, chat_id: int, **__: Any) -> str:
        self.calls.append(('run_dangerous_with_progress', {'chat_id': int(chat_id), 'prompt': str(prompt)}))
        return 'OK'


def _mk_router(
    *, api: _FakeAPI, state: BotState, codex: _FakeCodexRunner, repo_root: Path, choice_timeout_seconds: int = 0
) -> Router:
    return Router(
        api=api,  # type: ignore[arg-type]
        state=state,
        codex=codex,  # type: ignore[arg-type]
        watcher=object(),  # type: ignore[arg-type]
        workspaces=_FakeWorkspaces(repo_root),  # type: ignore[arg-type]
        owner_chat_id=1,
        router_mode='heuristic',
        min_profile='read',
        force_write_prefix='!',
        force_read_prefix='?',
        force_danger_prefix='∆',
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
        tg_voice_route_choice_timeout_seconds=int(choice_timeout_seconds),
        runtime_queue_snapshot=None,
        runtime_queue_drop=None,
        runtime_queue_mutate=None,
        runtime_queue_edit_active=None,
        runtime_queue_edit_set=None,
    )


class TestVoiceRoute(unittest.TestCase):
    def test_describe_callback_data_labels_common_buttons(self) -> None:
        self.assertEqual(keyboards.describe_callback_data(keyboards.CB_CX_PLAN3), '🧾 План 3 шага')
        self.assertEqual(
            keyboards.describe_callback_data(f'{keyboards.CB_DANGER_ALLOW_PREFIX}abc123'),
            '⚠️ Dangerous override: YES',
        )
        self.assertIn('Voice route', keyboards.describe_callback_data(f'{keyboards.CB_VOICE_ROUTE_PREFIX}123:r'))

    def test_voice_route_menu_callback_data_fits_limit(self) -> None:
        kb = keyboards.voice_route_menu(voice_message_id=123)
        rows = kb.get('inline_keyboard') or []
        self.assertTrue(rows)
        for row in rows:
            for btn in row:
                data = str(btn.get('callback_data') or '')
                self.assertTrue(data.startswith(keyboards.CB_VOICE_ROUTE_PREFIX))
                self.assertLessEqual(len(data.encode('utf-8')), 64)

    def test_state_pending_voice_route_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            st.init_pending_voice_route(chat_id=1, voice_message_id=10, ttl_seconds=60)
            self.assertIsNotNone(st.pending_voice_route(chat_id=1, voice_message_id=10))
            self.assertIsNone(st.pending_voice_route_choice(chat_id=1, voice_message_id=10))

            st.set_voice_route_choice(chat_id=1, voice_message_id=10, choice='read', ttl_seconds=60)
            self.assertEqual(st.pending_voice_route_choice(chat_id=1, voice_message_id=10), 'read')

            st.pop_pending_voice_route(chat_id=1, voice_message_id=10)
            self.assertIsNone(st.pending_voice_route(chat_id=1, voice_message_id=10))

    def test_router_callback_sets_choice_and_updates_keyboard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            codex = _FakeCodexRunner()
            router = _mk_router(api=api, state=st, codex=codex, repo_root=Path(td))

            router.handle_callback(
                chat_id=1,
                user_id=1,
                data=f'{keyboards.CB_VOICE_ROUTE_PREFIX}555:r',
                callback_query_id='cb',
                message_id=999,
            )

            self.assertEqual(st.pending_voice_route_choice(chat_id=1, voice_message_id=555), 'read')
            self.assertTrue(api.reply_markup_edits)
            last = api.reply_markup_edits[-1]
            self.assertEqual(last['message_id'], 999)
            self.assertIsInstance(last['reply_markup'], dict)

    def test_router_handle_text_applies_voice_route_read_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            # Simulate voice ack created a pending route selection.
            st.init_pending_voice_route(chat_id=1, voice_message_id=123, ttl_seconds=60)
            st.set_voice_route_choice(chat_id=1, voice_message_id=123, choice='read', ttl_seconds=60)

            api = _FakeAPI()
            codex = _FakeCodexRunner()
            router = _mk_router(api=api, state=st, codex=codex, repo_root=Path(td), choice_timeout_seconds=0)

            # "реализуй" would normally force write; the voice-route "read" should override it.
            router.handle_text(
                chat_id=1,
                user_id=1,
                text='реализуй пожалуйста',
                message_id=123,
                ack_message_id=777,
            )

            self.assertIsNone(st.pending_voice_route(chat_id=1, voice_message_id=123))
            self.assertTrue(any(c[0] == 'run_with_progress' and c[1]['automation'] is False for c in codex.calls))
            self.assertTrue(any(e['message_id'] == 777 and e['reply_markup'] is None for e in api.reply_markup_edits))

    def test_router_handle_text_waits_for_voice_route_choice_in_thread_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            voice_mid = 123
            thread_id = 42

            st.init_pending_voice_route(
                chat_id=1, message_thread_id=thread_id, voice_message_id=voice_mid, ttl_seconds=60
            )

            api = _FakeAPI()
            codex = _FakeCodexRunner()
            router = _mk_router(api=api, state=st, codex=codex, repo_root=Path(td), choice_timeout_seconds=2)

            def _late_choice() -> None:
                time.sleep(0.05)
                st.set_voice_route_choice(
                    chat_id=1,
                    message_thread_id=thread_id,
                    voice_message_id=voice_mid,
                    choice='read',
                    ttl_seconds=60,
                )

            t = threading.Thread(target=_late_choice, daemon=True)
            t.start()
            router.handle_text(
                chat_id=1,
                message_thread_id=thread_id,
                user_id=1,
                text='реализуй пожалуйста',
                message_id=voice_mid,
                ack_message_id=777,
            )
            t.join(timeout=2.0)

            self.assertIsNone(
                st.pending_voice_route(chat_id=1, message_thread_id=thread_id, voice_message_id=voice_mid)
            )
            self.assertTrue(any(c[0] == 'run_with_progress' and c[1]['automation'] is False for c in codex.calls))
