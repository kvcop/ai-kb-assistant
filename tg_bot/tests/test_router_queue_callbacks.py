import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tg_bot import keyboards
from tg_bot.router import Router
from tg_bot.state import BotState


class _FakeAPI:
    def __init__(self, *, edit_raises: str | None = None) -> None:
        self.edit_raises = edit_raises
        self.edits: list[dict[str, Any]] = []
        self.sends: list[dict[str, Any]] = []

    def answer_callback_query(self, *, callback_query_id: str, text: str | None = None) -> None:
        return None

    def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        if self.edit_raises:
            raise Exception(self.edit_raises)
        self.edits.append(
            {
                'chat_id': int(chat_id),
                'message_id': int(message_id),
                'text': str(text),
                'reply_markup': reply_markup,
            }
        )

    def send_message(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        reply_to_message_id: int | None = None,
        **__: Any,
    ) -> dict[str, Any]:
        self.sends.append(
            {
                'chat_id': int(chat_id),
                'text': str(text),
                'reply_markup': reply_markup,
                'reply_to_message_id': int(reply_to_message_id or 0),
            }
        )
        return {'ok': True, 'result': {'message_id': 1}}


def _mk_router(
    *,
    api: _FakeAPI,
    state: BotState,
    snapshot: Any,
    drop: Any,
    mutate: Any,
    edit_active: Any,
    edit_set: Any,
) -> Router:
    return Router(
        api=api,  # type: ignore[arg-type]
        state=state,
        codex=object(),  # type: ignore[arg-type]
        watcher=object(),  # type: ignore[arg-type]
        workspaces=object(),  # type: ignore[arg-type]
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
        runtime_queue_snapshot=snapshot,
        runtime_queue_drop=drop,
        runtime_queue_mutate=mutate,
        runtime_queue_edit_active=edit_active,
        runtime_queue_edit_set=edit_set,
    )


class TestRouterQueueCallbacks(unittest.TestCase):
    def test_admin_menu_renders(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            def snapshot(max_items: int) -> dict[str, Any]:
                return {
                    'in_flight': '',
                    'main_n': 0,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': [],
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=lambda *_: {'ok': False, 'error': 'not_used'},
                edit_active=lambda: False,
                edit_set=lambda _: None,
            )

            router.handle_callback(chat_id=1, user_id=1, data=keyboards.CB_ADMIN, callback_query_id='cb', message_id=10)
            self.assertTrue(api.edits)
            self.assertIn('🛠 Admin', api.edits[-1]['text'])
            self.assertIsInstance(api.edits[-1]['reply_markup'], dict)

    def test_admin_drop_queue_uses_drop_command(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            def snapshot(max_items: int) -> dict[str, Any]:
                return {
                    'in_flight': '',
                    'main_n': 0,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': [],
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            drops: list[str] = []

            def drop(kind: str) -> dict[str, Any]:
                drops.append(str(kind))
                return {'main': 1, 'prio': 0, 'paused': 0}

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=drop,
                mutate=lambda *_: {'ok': False, 'error': 'not_used'},
                edit_active=lambda: False,
                edit_set=lambda _: None,
            )

            router.handle_callback(
                chat_id=1, user_id=1, data=keyboards.CB_ADMIN_DROP_QUEUE, callback_query_id='cb', message_id=10
            )
            self.assertEqual(drops, ['queue'])
            self.assertIn('🧹 Dropped:', api.edits[-1]['text'])

    def test_queue_edit_and_done_toggle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            edit_state = {'active': False}

            def edit_active() -> bool:
                return bool(edit_state['active'])

            def edit_set(active: bool) -> None:
                edit_state['active'] = bool(active)

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                return {
                    'in_flight': '',
                    'main_n': 0,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': [] if lim <= 0 else [],
                    'prio_head': [] if lim <= 0 else [],
                    'paused_head': [] if lim <= 0 else [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=lambda *_: {'ok': False, 'error': 'not_used'},
                edit_active=edit_active,
                edit_set=edit_set,
            )

            router.handle_callback(
                chat_id=1, user_id=1, data=f'{keyboards.CB_QUEUE_EDIT_PREFIX}0', callback_query_id='cb', message_id=10
            )
            self.assertTrue(edit_state['active'])
            self.assertTrue(api.edits)
            self.assertIn('🧾 Queue (edit)', api.edits[-1]['text'])
            self.assertIn('Mode: EDIT', api.edits[-1]['text'])

            router.handle_callback(
                chat_id=1, user_id=1, data=f'{keyboards.CB_QUEUE_DONE_PREFIX}0', callback_query_id='cb', message_id=10
            )
            self.assertFalse(edit_state['active'])
            self.assertIn('🧾 Queue (read-only)', api.edits[-1]['text'])

    def test_queue_clear_calls_drop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            main = ['a', 'b']

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                return {
                    'in_flight': '',
                    'main_n': len(main),
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': (main[:lim] if lim > 0 else []),
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            drop_calls: list[str] = []

            def drop(kind: str) -> dict[str, Any]:
                drop_calls.append(str(kind))
                main.clear()
                return {'main': 2}

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=drop,
                mutate=lambda *_: {'ok': False, 'error': 'not_used'},
                edit_active=lambda: False,
                edit_set=lambda _: None,
            )

            router.handle_callback(
                chat_id=1, user_id=1, data=f'{keyboards.CB_QUEUE_CLEAR_PREFIX}0', callback_query_id='cb', message_id=10
            )
            self.assertEqual(drop_calls, ['queue'])
            self.assertIn('🧹 Cleared', api.edits[-1]['text'])
            self.assertIn('Main: 0', api.edits[-1]['text'])

    def test_queue_act_requires_edit_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                return {
                    'in_flight': '',
                    'main_n': 1,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': (['x'][:lim] if lim > 0 else []),
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=lambda *_: {'ok': True, 'changed': True},
                edit_active=lambda: False,
                edit_set=lambda _: None,
            )

            router.handle_callback(
                chat_id=1, user_id=1, data='queue_act:main:0:del:0', callback_query_id='cb', message_id=10
            )
            self.assertIn('⛔️ Edit mode is OFF', api.edits[-1]['text'])

    def test_queue_act_mutates_when_edit_on(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            edit_state = {'active': True}
            main = ['a', 'b']

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                return {
                    'in_flight': '',
                    'main_n': len(main),
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': (main[:lim] if lim > 0 else []),
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            def mutate(bucket: str, action: str, index: int) -> dict[str, Any]:
                if bucket != 'main':
                    return {'ok': False, 'error': 'readonly_bucket'}
                if action != 'down':
                    return {'ok': False, 'error': 'bad_action'}
                if index != 0:
                    return {'ok': False, 'error': 'bad_index'}
                main[0], main[1] = main[1], main[0]
                return {'ok': True, 'changed': True, 'n': len(main)}

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=mutate,
                edit_active=lambda: bool(edit_state['active']),
                edit_set=lambda active: edit_state.__setitem__('active', bool(active)),
            )

            router.handle_callback(
                chat_id=1, user_id=1, data='queue_act:main:0:down:0', callback_query_id='cb', message_id=10
            )
            text = api.edits[-1]['text']
            self.assertIn('🧾 Queue (edit)', text)
            self.assertIn('  1. [M] b', text)
            self.assertIn('  2. [M] a', text)

    def test_queue_page_includes_spool_items(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                head = ['spool text chat=1 mid=1: voice'][:lim] if lim > 0 else []
                return {
                    'in_flight': '',
                    'main_n': 0,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': [],
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 1,
                    'spool_head': head,
                    'spool_truncated': False,
                    'restart_pending': True,
                }

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=lambda *_: {'ok': False, 'error': 'not_used'},
                edit_active=lambda: False,
                edit_set=lambda _: None,
            )

            router.handle_callback(chat_id=1, user_id=1, data='queue:0', callback_query_id='cb', message_id=10)
            text = api.edits[-1]['text']
            self.assertIn('Spool: 1', text)
            self.assertIn('[S]', text)
            self.assertIn('spool text', text)
            self.assertNotIn('Очередь пуста', text)

    def test_queue_item_spool_renders_in_edit_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                head = ['spool text chat=1 mid=1: voice'][:lim] if lim > 0 else []
                return {
                    'in_flight': '',
                    'main_n': 0,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': [],
                    'prio_head': [],
                    'paused_head': [],
                    'spool_n': 1,
                    'spool_head': head,
                    'spool_truncated': False,
                    'restart_pending': True,
                }

            api = _FakeAPI()
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=lambda *_: {'ok': True, 'changed': True},
                edit_active=lambda: True,
                edit_set=lambda _: None,
            )

            router.handle_callback(
                chat_id=1,
                user_id=1,
                data=f'{keyboards.CB_QUEUE_ITEM_PREFIX}spool:0:0',
                callback_query_id='cb',
                message_id=10,
            )
            text = api.edits[-1]['text']
            self.assertIn('Bucket: spool', text)
            kb = api.edits[-1]['reply_markup']
            self.assertIsInstance(kb, dict)
            btn_data = [
                b.get('callback_data')
                for row in (kb.get('inline_keyboard') or [])
                for b in (row or [])
                if isinstance(b, dict)
            ]
            self.assertIn('queue_act:spool:0:del:0', btn_data)

    def test_message_not_modified_is_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            def snapshot(max_items: int) -> dict[str, Any]:
                lim = max(0, int(max_items))
                return {
                    'in_flight': '',
                    'main_n': 0,
                    'prio_n': 0,
                    'paused_n': 0,
                    'main_head': [] if lim <= 0 else [],
                    'prio_head': [] if lim <= 0 else [],
                    'paused_head': [] if lim <= 0 else [],
                    'spool_n': 0,
                    'spool_truncated': False,
                    'restart_pending': False,
                }

            api = _FakeAPI(edit_raises='Bad Request: message is not modified')
            router = _mk_router(
                api=api,
                state=st,
                snapshot=snapshot,
                drop=lambda _: {},
                mutate=lambda *_: {'ok': False, 'error': 'not_used'},
                edit_active=lambda: False,
                edit_set=lambda _: None,
            )

            router.handle_callback(chat_id=1, user_id=1, data='queue:0', callback_query_id='cb', message_id=10)
            self.assertEqual(api.sends, [])
