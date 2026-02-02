import re
import tempfile
import unittest
from pathlib import Path

from tg_bot.router import Router
from tg_bot.state import BotState


class TestRouterMcpNote(unittest.TestCase):
    def test_wrap_user_prompt_includes_mcp_note_when_requested(self) -> None:
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

            out = router._wrap_user_prompt('mcp доступны?', chat_id=1)
            self.assertIn('Примечание по MCP:', out)
            self.assertIn('mcp__server-memory__read_graph', out)

            out2 = router._wrap_user_prompt('привет', chat_id=1)
            self.assertNotIn('Примечание по MCP:', out2)
