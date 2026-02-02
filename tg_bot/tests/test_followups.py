import re
import tempfile
import unittest
from pathlib import Path

from tg_bot.router import Router
from tg_bot.state import BotState


class TestFollowupsState(unittest.TestCase):
    def test_record_pending_followup_persists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            st.record_pending_followup(
                chat_id=1,
                message_thread_id=2,
                message_id=100,
                user_id=10,
                received_ts=123.0,
                text=' hello ',
                attachments=[{'path': 'tg_uploads/x.txt', 'name': 'x.txt'}],
                reply_to={'message_id': 99, 'text': 'prev'},
            )

            st2 = BotState(path=state_path)
            st2.load()
            items = st2.pending_followups_by_scope.get('1:2') or []
            self.assertEqual(len(items), 1)
            self.assertEqual(int(items[0].get('message_id') or 0), 100)
            self.assertEqual(int(items[0].get('user_id') or 0), 10)
            self.assertEqual(str(items[0].get('text') or ''), 'hello')
            self.assertTrue(isinstance(items[0].get('attachments'), list))
            self.assertTrue(isinstance(items[0].get('reply_to'), dict))

    def test_record_pending_followup_prunes_by_max_items(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            for i in range(5):
                st.record_pending_followup(
                    chat_id=1,
                    message_thread_id=0,
                    message_id=100 + i,
                    user_id=10,
                    received_ts=123.0 + i,
                    text=f'm{i}',
                    max_items_per_scope=2,
                )

            items = st.pending_followups_by_scope.get('1:0') or []
            self.assertEqual([int(x.get('message_id') or 0) for x in items], [103, 104])


class TestFollowupsPromptHints(unittest.TestCase):
    def test_wrap_user_prompt_includes_followups_hints_only_for_owner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            router = Router(
                api=object(),  # type: ignore[arg-type]
                state=st,
                codex=object(),  # type: ignore[arg-type]
                watcher=object(),  # type: ignore[arg-type]
                workspaces=object(),  # type: ignore[arg-type]
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

            out_owner = router._wrap_user_prompt('привет', chat_id=1)
            self.assertIn('Telegram (MCP):', out_owner)
            self.assertIn('mcp__telegram-send__send_message', out_owner)
            self.assertIn('mcp__telegram-send__send_files', out_owner)
            self.assertIn('Telegram follow-ups (MCP):', out_owner)
            self.assertIn('mcp__telegram-followups__get_followups', out_owner)
            self.assertNotIn('mcp__telegram-send__get_followups', out_owner)

            out_other = router._wrap_user_prompt('привет', chat_id=2)
            self.assertNotIn('Telegram (MCP):', out_other)
            self.assertNotIn('Telegram follow-ups (MCP):', out_other)


class TestFollowupsMcpConfigOverrides(unittest.TestCase):
    def test_codex_mcp_config_overrides_enable_followups_only_for_owner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / 'scripts').mkdir(parents=True, exist_ok=True)
            (repo_root / 'scripts' / 'mcp_telegram_followups.py').write_text('# stub\n', encoding='utf-8')

            state_path = repo_root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            router = Router(
                api=object(),  # type: ignore[arg-type]
                state=st,
                codex=object(),  # type: ignore[arg-type]
                watcher=object(),  # type: ignore[arg-type]
                workspaces=object(),  # type: ignore[arg-type]
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

            # Default: enabled.
            o_owner = router._codex_mcp_config_overrides(chat_id=1, repo_root=repo_root)
            self.assertEqual(o_owner.get('mcp_servers.telegram-send.env.TG_MCP_SENDER_ENABLED'), '1')
            self.assertEqual(o_owner.get('mcp_servers.telegram-send.env.TG_MCP_FOLLOWUPS_ENABLED'), '0')
            self.assertEqual(o_owner.get('mcp_servers.telegram-followups.command'), 'python3')
            self.assertIn('mcp_servers.telegram-followups.args', o_owner)

            # Non-owner chat: followups MCP is not added.
            o_other = router._codex_mcp_config_overrides(chat_id=2, repo_root=repo_root)
            self.assertIn('mcp_servers.telegram-send.env.TG_MCP_FOLLOWUPS_ENABLED', o_other)
            self.assertNotIn('mcp_servers.telegram-followups.command', o_other)

            # Toggle OFF: followups MCP is not added even for owner.
            st.ux_set_mcp_live_enabled(chat_id=1, value=False)
            o_off = router._codex_mcp_config_overrides(chat_id=1, repo_root=repo_root)
            self.assertIn('mcp_servers.telegram-send.env.TG_MCP_FOLLOWUPS_ENABLED', o_off)
            self.assertNotIn('mcp_servers.telegram-followups.command', o_off)
