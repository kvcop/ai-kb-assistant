import re
import time
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

    def answer_callback_query(self, *_: object, **__: object) -> None:
        return None

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


def _model_callback_data(api: _FakeAPI) -> list[str]:
    markup = api.sent[-1].get('reply_markup')
    rows = []
    if isinstance(markup, dict):
        rows = markup.get('inline_keyboard', [])
    data: list[str] = []
    for row in rows:
        for btn in row:
            if isinstance(btn, dict):
                value = btn.get('callback_data')
                if isinstance(value, str):
                    data.append(value)
    return data


class TestRouterCollectCommands(unittest.TestCase):
    def test_sleep_command_show_set_off_by_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/sleep', message_id=900)
            self.assertIn('😴 Sleep: OFF', _last_text(api))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/sleep 23:45', message_id=901)
            self.assertIn('😴 Ок. Sleep установлен до', _last_text(api))

            sleep_ts = st.sleep_until(chat_id=1, message_thread_id=7)
            self.assertGreater(sleep_ts, 0.0)
            # Parsed time may wrap to tomorrow, but must be in the future relative to now.
            self.assertGreater(sleep_ts, time.time())

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/sleep off', message_id=902)
            self.assertIn('😴 Sleep: OFF.', _last_text(api))
            self.assertEqual(st.sleep_until(chat_id=1, message_thread_id=7), 0.0)

    def test_sleep_command_format_validation_and_off_alias(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/sleep 99:99', message_id=910)
            self.assertIn('Неверный формат времени', _last_text(api))
            self.assertEqual(st.sleep_until(chat_id=1, message_thread_id=7), 0.0)

            st.set_sleep_until(chat_id=1, message_thread_id=7, until_ts=1_800_000.0)
            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/sleep 0', message_id=911)
            self.assertIn('😴 Sleep: OFF.', _last_text(api))
            self.assertEqual(st.sleep_until(chat_id=1, message_thread_id=7), 0.0)
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

    def test_collect_retry_blocked_while_active_and_clears_to_deferred(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            st.append(chat_id=1, message_thread_id=7, item={'id': 'task-1', 'text': 'first'})
            st.append(chat_id=1, message_thread_id=7, item={'id': 'task-2', 'text': 'second'})

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect start', message_id=300)
            self.assertIn('collect start: активен item task-1', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect retry', message_id=301)
            self.assertIn('collect retry: сначала завершите или отмените текущий active item.', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect done', message_id=302)
            self.assertIn('collect done: active item task-1 завершён.', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'pending')
            self.assertEqual(len(st.collect_pending.get('1:7') or []), 1)
            deferred_items = st.collect_deferred.get('1:7')
            self.assertIsInstance(deferred_items, list)
            self.assertEqual(len(deferred_items), 1)
            self.assertEqual(deferred_items[0].get('id'), 'task-1')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect start', message_id=303)
            self.assertIn('collect start: активен item task-2', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'active')

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect cancel', message_id=304)
            self.assertIn('collect cancel: active item task-2 отменён.', _last_text(api))
            self.assertEqual(st.status(chat_id=1, message_thread_id=7), 'deferred')
            deferred_items = st.collect_deferred.get('1:7')
            self.assertIsInstance(deferred_items, list)
            self.assertEqual([x.get('id') for x in deferred_items], ['task-1'])

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/collect retry', message_id=305)
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

    def test_profile_cycle_commands_persist_per_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=0, user_id=42, text='/plan', message_id=400)
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=0), ('read', None, 'medium'))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/implement', message_id=401)
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('write', None, 'high'))
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=0), ('read', None, 'medium'))

            router.handle_text(chat_id=1, message_thread_id=0, user_id=42, text='/implement', message_id=402)
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=0), ('write', None, 'high'))
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('write', None, 'high'))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/review', message_id=403)
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('read', None, 'high'))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/model gpt-4.1', message_id=404)
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('read', 'gpt-4.1', 'high'))
            self.assertIn('gpt-4.1', _last_text(api))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/model', message_id=405)
            last_text = _last_text(api)
            self.assertIn('mode: read', last_text)
            self.assertIn('reasoning: high', last_text)
            self.assertIn('model: gpt-4.1', last_text)

    def test_model_command_shows_inline_menu_and_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/model', message_id=500)
            self.assertIn('scope: 1:7', _last_text(api))
            self.assertIn('model:__default__', _model_callback_data(api))

            btn_data = _model_callback_data(api)
            self.assertTrue(any(x.startswith('model:') for x in btn_data))

    def test_profile_model_callback_updates_only_model_within_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/review', message_id=601)
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7)[0], 'read')
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7)[2], 'high')

            router.handle_callback(
                chat_id=1, message_thread_id=7, user_id=42, data='model:gpt-4.1', callback_query_id='cb', message_id=602
            )
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('read', 'gpt-4.1', 'high'))

            router.handle_callback(
                chat_id=1, message_thread_id=7, user_id=42, data='model:__default__', callback_query_id='cb', message_id=603
            )
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('read', None, 'high'))

    def test_model_command_shows_global_root_model_for_topic_without_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.set_last_codex_profile_state(
                chat_id=1,
                message_thread_id=0,
                mode='read',
                model='gpt-root',
                reasoning='medium',
            )
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/model', message_id=700)
            self.assertIn('model: gpt-root', _last_text(api))

    def test_model_default_in_topic_keeps_root_model_for_topic_state_and_display(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.set_last_codex_profile_state(
                chat_id=1,
                message_thread_id=0,
                mode='read',
                model='gpt-root',
                reasoning='medium',
            )
            st.set_last_codex_profile_state(
                chat_id=1,
                message_thread_id=7,
                mode='read',
                model='gpt-topic',
                reasoning='high',
            )
            api = _FakeAPI()
            router = _mk_router(api=api, state=st, root=root)

            router.handle_callback(
                chat_id=1,
                message_thread_id=7,
                user_id=42,
                data='model:__default__',
                callback_query_id='cb-topic-default',
                message_id=701,
            )
            self.assertEqual(st.last_codex_profile_state_for(chat_id=1, message_thread_id=7), ('read', 'gpt-root', 'high'))

            router.handle_text(chat_id=1, message_thread_id=7, user_id=42, text='/model', message_id=702)
            self.assertIn('model: gpt-root', _last_text(api))
