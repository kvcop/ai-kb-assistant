import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_bot.config import BotConfig
from tg_bot.mattermost_watch import MattermostWatcher, _MMChannel


class TestMattermostChannelTitles(unittest.TestCase):
    def _cfg(self, *, repo_root: Path) -> BotConfig:
        env = {
            'TG_REPO_ROOT': str(repo_root),
            'TG_BOT_TOKEN': 'test-token',
        }
        with patch.dict(os.environ, env, clear=False):
            return BotConfig.from_env()

    def test_dm_title_resolves_to_username(self) -> None:
        me_id = 'm' * 26
        other_id = 'o' * 26

        with tempfile.TemporaryDirectory() as td:
            cfg = self._cfg(repo_root=Path(td))
            w = MattermostWatcher(cfg)
            w._user_by_id[other_id] = {'id': other_id, 'username': 'a.shuvalov'}
            ch = _MMChannel(
                id='chan',
                display_name=f'{me_id}__{other_id}',
                name=f'{me_id}__{other_id}',
                team_id='',
                type='D',
            )

            header = w._mm_header(team=None, ch=ch, me_id=me_id)
            self.assertEqual(header, '🟣 Mattermost — Личка: @a.shuvalov')

    def test_group_dm_title_shows_top3_and_more_count(self) -> None:
        me_id = 'm' * 26
        u1 = 'a' * 26
        u2 = 'b' * 26
        u3 = 'c' * 26
        u4 = 'd' * 26

        with tempfile.TemporaryDirectory() as td:
            cfg = self._cfg(repo_root=Path(td))
            w = MattermostWatcher(cfg)
            w._user_by_id[u1] = {'id': u1, 'username': 'u1'}
            w._user_by_id[u2] = {'id': u2, 'username': 'u2'}
            w._user_by_id[u3] = {'id': u3, 'username': 'u3'}
            w._user_by_id[u4] = {'id': u4, 'username': 'u4'}

            ch = _MMChannel(
                id='chan',
                display_name=f'{me_id}__{u1}__{u2}__{u3}__{u4}',
                name=f'{me_id}__{u1}__{u2}__{u3}__{u4}',
                team_id='',
                type='G',
            )

            header = w._mm_header(team=None, ch=ch, me_id=me_id)
            self.assertEqual(header, '🟣 Mattermost — Группа: @u1, @u2, @u3 (+1)')
