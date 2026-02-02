import json
import tempfile
import unittest
from pathlib import Path

from tg_bot import keyboards
from tg_bot.state import BotState


class TestSettings(unittest.TestCase):
    def test_state_settings_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'state.json'
            path.write_text('{}', encoding='utf-8')
            st = BotState(path=path)
            st.load()

            self.assertTrue(st.ux_prefer_edit_delivery(chat_id=123))
            self.assertTrue(st.ux_done_notice_enabled(chat_id=123))
            self.assertEqual(st.ux_done_notice_delete_seconds(chat_id=123), 300)
            self.assertTrue(st.ux_bot_initiatives_enabled(chat_id=123))
            self.assertFalse(st.ux_live_chatter_enabled(chat_id=123))
            self.assertTrue(st.ux_mcp_live_enabled(chat_id=123))
            self.assertTrue(st.ux_user_in_loop_enabled(chat_id=123))

    def test_state_settings_persist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'state.json'
            path.write_text('{}', encoding='utf-8')
            st = BotState(path=path)
            st.load()
            st.ux_set_prefer_edit_delivery(chat_id=1, value=False)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)
            st.ux_set_done_notice_delete_seconds(chat_id=1, seconds=60)
            st.ux_set_bot_initiatives_enabled(chat_id=1, value=False)
            st.ux_set_live_chatter_enabled(chat_id=1, value=True)
            st.ux_set_mcp_live_enabled(chat_id=1, value=False)
            st.ux_set_user_in_loop_enabled(chat_id=1, value=False)

            st2 = BotState(path=path)
            st2.load()
            self.assertFalse(st2.ux_prefer_edit_delivery(chat_id=1))
            self.assertFalse(st2.ux_done_notice_enabled(chat_id=1))
            self.assertEqual(st2.ux_done_notice_delete_seconds(chat_id=1), 60)
            self.assertFalse(st2.ux_bot_initiatives_enabled(chat_id=1))
            self.assertTrue(st2.ux_live_chatter_enabled(chat_id=1))
            self.assertFalse(st2.ux_mcp_live_enabled(chat_id=1))
            self.assertFalse(st2.ux_user_in_loop_enabled(chat_id=1))

    def test_state_settings_ttl_clamps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'state.json'
            path.write_text('{}', encoding='utf-8')
            st = BotState(path=path)
            st.load()

            st.ux_set_done_notice_delete_seconds(chat_id=1, seconds=-5)
            self.assertEqual(st.ux_done_notice_delete_seconds(chat_id=1), 0)

            st.ux_set_done_notice_delete_seconds(chat_id=1, seconds=999999)
            self.assertEqual(st.ux_done_notice_delete_seconds(chat_id=1), 24 * 60 * 60)

    def test_settings_menu_structure(self) -> None:
        kb = keyboards.settings_menu(
            prefer_edit_delivery=True,
            done_notice_enabled=True,
            done_notice_delete_seconds=300,
            bot_initiatives_enabled=True,
            live_chatter_enabled=True,
            mcp_live_enabled=True,
            user_in_loop_enabled=True,
        )
        self.assertIsInstance(kb, dict)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)
        self.assertGreaterEqual(len(rows), 2)

        btn_texts = [b.get('text') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertTrue(any(isinstance(t, str) and 'Edit ✅' in t for t in btn_texts))
        self.assertTrue(any(isinstance(t, str) and 'Done: 5м' in t for t in btn_texts))

        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertIn(keyboards.CB_SETTINGS_DELIVERY_EDIT, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_DELIVERY_NEW, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_DONE_TOGGLE, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_DONE_TTL_CYCLE, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_BOT_INITIATIVES_TOGGLE, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_LIVE_CHATTER_TOGGLE, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_MCP_LIVE_TOGGLE, btn_data)
        self.assertIn(keyboards.CB_SETTINGS_USER_IN_LOOP_TOGGLE, btn_data)

        for d in btn_data:
            if isinstance(d, str):
                self.assertLessEqual(len(d.encode('utf-8')), 64)

    def test_settings_callbacks_are_control_plane(self) -> None:
        for cb in {
            keyboards.CB_SETTINGS,
            keyboards.CB_SETTINGS_DELIVERY_EDIT,
            keyboards.CB_SETTINGS_DELIVERY_NEW,
            keyboards.CB_SETTINGS_DONE_TOGGLE,
            keyboards.CB_SETTINGS_DONE_TTL_CYCLE,
            keyboards.CB_SETTINGS_BOT_INITIATIVES_TOGGLE,
            keyboards.CB_SETTINGS_LIVE_CHATTER_TOGGLE,
            keyboards.CB_SETTINGS_MCP_LIVE_TOGGLE,
            keyboards.CB_SETTINGS_USER_IN_LOOP_TOGGLE,
        }:
            self.assertIn(cb, keyboards.CONTROL_PLANE_CALLBACK_DATA)

    def test_state_save_includes_settings_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'state.json'
            st = BotState(path=path)
            st.ux_set_prefer_edit_delivery(chat_id=1, value=False)
            st.ux_set_done_notice_enabled(chat_id=1, value=False)
            st.ux_set_done_notice_delete_seconds(chat_id=1, seconds=60)
            st.ux_set_bot_initiatives_enabled(chat_id=1, value=False)
            st.ux_set_live_chatter_enabled(chat_id=1, value=True)
            st.ux_set_mcp_live_enabled(chat_id=1, value=False)
            st.ux_set_user_in_loop_enabled(chat_id=1, value=False)

            raw = json.loads(path.read_text(encoding='utf-8'))
            self.assertIn('ux_prefer_edit_delivery_by_chat', raw)
            self.assertIn('ux_done_notice_enabled_by_chat', raw)
            self.assertIn('ux_done_notice_delete_seconds_by_chat', raw)
            self.assertIn('ux_bot_initiatives_enabled_by_chat', raw)
            self.assertIn('ux_live_chatter_enabled_by_chat', raw)
            self.assertIn('ux_mcp_live_enabled_by_chat', raw)
            self.assertIn('ux_user_in_loop_enabled_by_chat', raw)
