import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_bot.config import BotConfig
from tg_bot.mattermost_watch import MattermostWatcher


class _DummyClient:
    def __init__(self, *, channels_all: list[dict[str, str]]) -> None:
        self._channels_all = channels_all

    def get(self, path: str) -> list[dict[str, str]]:
        if path.endswith('/channels') and '/users/' in path:
            return self._channels_all
        raise AssertionError(f'Unexpected path: {path}')


class _DummyTeams:
    def __init__(self, *, by_name: dict[str, dict[str, str]]) -> None:
        self._by_name = by_name

    def get_team_by_name(self, name: str) -> dict[str, str]:
        if name in self._by_name:
            return self._by_name[name]
        raise KeyError(name)


class _DummyChannels:
    def __init__(self, *, by_team_id: dict[str, list[dict[str, str]]]) -> None:
        self._by_team_id = by_team_id

    def get_channels_for_user(self, user_id: str, team_id: str) -> list[dict[str, str]]:
        _ = user_id
        return self._by_team_id.get(team_id, [])


class _DummyDriver:
    def __init__(
        self,
        *,
        channels_all: list[dict[str, str]],
        teams_by_name: dict[str, dict[str, str]],
        channels_by_team_id: dict[str, list[dict[str, str]]],
    ) -> None:
        self.client = _DummyClient(channels_all=channels_all)
        self.teams = _DummyTeams(by_name=teams_by_name)
        self.channels = _DummyChannels(by_team_id=channels_by_team_id)


class TestMattermostChannelSelection(unittest.TestCase):
    def _cfg(self, *, repo_root: Path, env: dict[str, str]) -> BotConfig:
        base_env = {
            'TG_REPO_ROOT': str(repo_root),
            'TG_BOT_TOKEN': 'test-token',
        }
        with patch.dict(os.environ, {**base_env, **env}, clear=False):
            return BotConfig.from_env()

    def test_explicit_team_scope_prioritizes_team_channels_before_dms(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = self._cfg(
                repo_root=Path(td),
                env={
                    'MM_TEAM_NAMES': 'rnd',
                    'MM_MAX_CHANNELS': '2',
                    'MM_INCLUDE_DMS': '1',
                },
            )
            w = MattermostWatcher(cfg)
            w._me = {'id': 'u1'}
            w._driver = _DummyDriver(
                channels_all=[
                    {'id': 'dm1', 'type': 'D', 'name': 'dm1'},
                    {'id': 'dm2', 'type': 'D', 'name': 'dm2'},
                ],
                teams_by_name={'rnd': {'id': 't1', 'name': 'rnd', 'display_name': 'RnD'}},
                channels_by_team_id={
                    't1': [
                        {'id': 'ch-team', 'type': 'O', 'name': 'team', 'display_name': 'Team'},
                        {'id': 'ch-dm-in-team', 'type': 'D', 'name': 'dm-in-team'},
                    ]
                },
            )

            ids = w._iter_channel_ids()
            self.assertEqual(ids, ['ch-team', 'dm1'])

    def test_explicit_team_scope_includes_extra_channel_ids_before_dms(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = self._cfg(
                repo_root=Path(td),
                env={
                    'MM_TEAM_NAMES': 'rnd',
                    'MM_MAX_CHANNELS': '3',
                    'MM_INCLUDE_DMS': '1',
                    'MM_EXTRA_CHANNEL_IDS': 'extra-1,extra-2',
                },
            )
            w = MattermostWatcher(cfg)
            w._me = {'id': 'u1'}
            w._driver = _DummyDriver(
                channels_all=[
                    {'id': 'dm1', 'type': 'D', 'name': 'dm1'},
                    {'id': 'dm2', 'type': 'D', 'name': 'dm2'},
                ],
                teams_by_name={'rnd': {'id': 't1', 'name': 'rnd', 'display_name': 'RnD'}},
                channels_by_team_id={'t1': [{'id': 'ch-team', 'type': 'O', 'name': 'team'}]},
            )

            ids = w._iter_channel_ids()
            self.assertEqual(ids, ['ch-team', 'extra-1', 'extra-2'])

    def test_explicit_channel_ids_includes_extra_channel_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = self._cfg(
                repo_root=Path(td),
                env={
                    'MM_CHANNEL_IDS': 'a,b',
                    'MM_EXTRA_CHANNEL_IDS': 'c',
                    'MM_MAX_CHANNELS': '10',
                },
            )
            w = MattermostWatcher(cfg)
            w._me = {'id': 'u1'}
            w._driver = _DummyDriver(channels_all=[], teams_by_name={}, channels_by_team_id={})

            ids = w._iter_channel_ids()
            self.assertEqual(ids, ['a', 'b', 'c'])
