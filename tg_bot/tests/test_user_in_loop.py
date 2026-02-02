import re
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from tg_bot.router import Router
from tg_bot.state import BotState


class _FakeAPI:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.edited: list[dict[str, Any]] = []
        self._next_message_id = 1000

    def send_chat_action(self, *_: Any, **__: Any) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        mid = self._next_message_id
        self._next_message_id += 1
        rec = dict(kwargs)
        rec['message_id'] = int(mid)
        self.sent.append(rec)
        return {'ok': True, 'result': {'message_id': mid}}

    def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        self.edited.append(dict(kwargs))
        return {'ok': True, 'result': True}

    def edit_message_reply_markup(self, **_: Any) -> dict[str, Any]:
        return {'ok': True, 'result': True}

    def delete_message(self, **_: Any) -> dict[str, Any]:
        return {'ok': True, 'result': True}


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
        self.calls = 0
        self.chat_profile = _Profile(name='chat', sandbox='read-only', full_auto=False)
        self.auto_profile = _Profile(name='auto', sandbox=None, full_auto=True)
        self.danger_profile = _Profile(name='danger', sandbox='danger-full-access', full_auto=False)

    def log_note(self, *_: Any, **__: Any) -> None:
        return None

    def run_with_progress(self, *, prompt: str, **_: Any) -> str:
        self.calls += 1
        if self.calls == 1:
            return (
                'Есть развилка по UX.\n\n'
                '```tg_bot\n'
                '{"ask_user": {"question": "Выбираем A или B?", "options": ["A", "B"], "default": "A"}}\n'
                '```'
            )
        assert 'Ответ на blocking-вопрос' in prompt
        assert 'Вопрос: Выбираем A или B?' in prompt
        assert 'Ответ: B' in prompt
        return 'OK'


class _FakeCodexRunnerOK:
    def __init__(self) -> None:
        self.prompts: list[str] = []
        self.chat_profile = _Profile(name='chat', sandbox='read-only', full_auto=False)
        self.auto_profile = _Profile(name='auto', sandbox=None, full_auto=True)
        self.danger_profile = _Profile(name='danger', sandbox='danger-full-access', full_auto=False)

    def log_note(self, *_: Any, **__: Any) -> None:
        return None

    def run_with_progress(self, *, prompt: str, **_: Any) -> str:
        self.prompts.append(str(prompt))
        return 'OK'


class TestWaitingForUserState(unittest.TestCase):
    def test_waiting_for_user_persists_and_clamps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'state.json'
            path.write_text('{}', encoding='utf-8')
            st = BotState(path=path)
            st.load()

            st.set_waiting_for_user(
                chat_id=1,
                message_thread_id=2,
                job={
                    'asked_ts': 123.0,
                    'question': 'Q',
                    'default': 'D',
                    'options': ['A', 'B', 'C', 'D', 'E', 'F', 'G'],
                    'ping_count': 99,
                    'last_ping_ts': 456.0,
                    'mode': 'WRITE',
                    'origin_message_id': 10,
                    'origin_ack_message_id': 11,
                    'origin_user_id': 12,
                },
            )

            st2 = BotState(path=path)
            st2.load()
            job = st2.waiting_for_user(chat_id=1, message_thread_id=2)
            assert job is not None
            self.assertEqual(job.get('question'), 'Q')
            self.assertEqual(job.get('default'), 'D')
            self.assertEqual(int(job.get('ping_count') or 0), 3)
            self.assertEqual(float(job.get('last_ping_ts') or 0.0), 456.0)
            self.assertEqual(job.get('mode'), 'write')
            self.assertEqual(int(job.get('origin_message_id') or 0), 10)
            self.assertEqual(int(job.get('origin_ack_message_id') or 0), 11)
            self.assertEqual(int(job.get('origin_user_id') or 0), 12)
            self.assertEqual(job.get('options'), ['A', 'B', 'C', 'D', 'E'])


class TestRouterAskUserResume(unittest.TestCase):
    def test_router_ask_user_sets_waiting_and_resumes_on_callback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=False)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            api = _FakeAPI()
            codex = _FakeCodexRunner()

            router = Router(
                api=api,  # type: ignore[arg-type]
                state=st,
                codex=codex,  # type: ignore[arg-type]
                watcher=object(),  # type: ignore[arg-type]
                workspaces=_FakeWorkspaces(root),  # type: ignore[arg-type]
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

            router.handle_text(chat_id=1, message_thread_id=123, user_id=1, text='hello', message_id=10)
            self.assertTrue(st.is_waiting_for_user(chat_id=1, message_thread_id=123))

            question_msgs = [m for m in api.sent if isinstance(m.get('text'), str) and '❓' in str(m.get('text'))]
            self.assertTrue(question_msgs)
            self.assertEqual(question_msgs[-1].get('message_thread_id'), 123)
            self.assertEqual(int(question_msgs[-1].get('reply_to_message_id') or 0), 10)

            reply_markup = question_msgs[-1].get('reply_markup')
            self.assertIsInstance(reply_markup, dict)
            kb = reply_markup.get('inline_keyboard') if isinstance(reply_markup, dict) else None
            self.assertIsInstance(kb, list)
            cb_data = []
            for row in kb or []:
                if not isinstance(row, list):
                    continue
                for btn in row:
                    if isinstance(btn, dict) and isinstance(btn.get('callback_data'), str):
                        cb_data.append(btn.get('callback_data'))
            self.assertIn('asku:1', cb_data)
            self.assertIn('asku:2', cb_data)
            self.assertIn('asku:def', cb_data)

            question_mid = int(question_msgs[-1].get('message_id') or 0)
            router.handle_callback(
                chat_id=1,
                message_thread_id=123,
                user_id=1,
                data='asku:2',
                callback_query_id='cb1',
                message_id=question_mid,
            )
            self.assertFalse(st.is_waiting_for_user(chat_id=1, message_thread_id=123))

            self.assertGreaterEqual(codex.calls, 2)
            final_msgs = [m for m in api.sent if isinstance(m.get('text'), str) and 'OK' in str(m.get('text'))]
            self.assertTrue(final_msgs)


class TestRouterNewCommand(unittest.TestCase):
    def test_new_command_cancels_waiting_and_starts_new_task(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=False)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)

            st.set_waiting_for_user(
                chat_id=1,
                message_thread_id=123,
                job={
                    'asked_ts': float(time.time()),
                    'question': 'Вопрос?',
                    'default': '',
                    'options': ['A', 'B'],
                    'ping_count': 0,
                    'last_ping_ts': 0.0,
                    'mode': 'read',
                    'origin_message_id': 10,
                    'origin_ack_message_id': 11,
                    'origin_user_id': 1,
                },
            )

            api = _FakeAPI()
            codex = _FakeCodexRunnerOK()

            router = Router(
                api=api,  # type: ignore[arg-type]
                state=st,
                codex=codex,  # type: ignore[arg-type]
                watcher=object(),  # type: ignore[arg-type]
                workspaces=_FakeWorkspaces(root),  # type: ignore[arg-type]
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

            router.handle_text(chat_id=1, message_thread_id=123, user_id=1, text='/new Сделай новое', message_id=12)
            self.assertFalse(st.is_waiting_for_user(chat_id=1, message_thread_id=123))

            new_msgs = [m for m in api.sent if isinstance(m.get('text'), str) and '🆕' in str(m.get('text'))]
            self.assertTrue(new_msgs)
            self.assertTrue(codex.prompts)
            self.assertIn('Сделай новое', codex.prompts[-1])
            self.assertNotIn('Ответ на blocking-вопрос', codex.prompts[-1])
