import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_bot.config import BotConfig


class TestConfigParsing(unittest.TestCase):
    def _base_env(self, *, repo_root: Path) -> dict[str, str]:
        return {
            'TG_REPO_ROOT': str(repo_root),
            'TG_BOT_TOKEN': 'test-token',
        }

    def test_tg_max_parallel_jobs_clamps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)

            with patch.dict(os.environ, {**self._base_env(repo_root=root), 'TG_MAX_PARALLEL_JOBS': '0'}, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.tg_max_parallel_jobs, 1)

            with patch.dict(os.environ, {**self._base_env(repo_root=root), 'TG_MAX_PARALLEL_JOBS': '999'}, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.tg_max_parallel_jobs, 20)

    def test_watch_idle_stage_minutes_cleaning_and_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)

            env = {
                **self._base_env(repo_root=root),
                'WATCH_IDLE_MINUTES': '120',
                'WATCH_ACK_MINUTES': '20',
                'WATCH_IDLE_STAGE_MINUTES': '120, 140, -1, abc, 130, 170',
            }
            with patch.dict(os.environ, env, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.watch_idle_stage_minutes, [120, 140, 170])

            env2 = {
                **self._base_env(repo_root=root),
                'WATCH_IDLE_MINUTES': '120',
                'WATCH_ACK_MINUTES': '20',
                'WATCH_IDLE_STAGE_MINUTES': '-1,0,abc',
            }
            with patch.dict(os.environ, env2, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.watch_idle_stage_minutes, [120, 140, 170, 230, 320])

    def test_tg_codex_parse_mode_can_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            env = {
                **self._base_env(repo_root=root),
                'TG_CODEX_PARSE_MODE': 'none',
            }
            with patch.dict(os.environ, env, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.tg_codex_parse_mode, '')

    def test_mm_extra_channel_ids_parses(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            env = {
                **self._base_env(repo_root=root),
                'MM_EXTRA_CHANNEL_IDS': 'a, b, ,c',
            }
            with patch.dict(os.environ, env, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.mm_extra_channel_ids, ['a', 'b', 'c'])

    def test_collect_payload_limits_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            env = {
                **self._base_env(repo_root=root),
                'TG_COLLECT_MAX_PAYLOAD_CHARS': '5555',
                'TG_COLLECT_MAX_ITEMS': '7',
                'TG_COLLECT_MAX_METADATA_CHARS': '777',
            }
            with patch.dict(os.environ, env, clear=False):
                cfg = BotConfig.from_env()
                self.assertEqual(cfg.tg_collect_max_payload_chars, 5555)
                self.assertEqual(cfg.tg_collect_max_items, 7)
                self.assertEqual(cfg.tg_collect_max_metadata_chars, 777)
