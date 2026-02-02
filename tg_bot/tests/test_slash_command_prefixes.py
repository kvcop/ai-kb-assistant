import unittest

from tg_bot.app import Event, _event_is_restart, _normalize_cmd_token, _spool_record_is_restart


class TestSlashCommandPrefixes(unittest.TestCase):
    def test_normalize_cmd_token_strips_router_force_prefixes(self) -> None:
        self.assertEqual(_normalize_cmd_token('/restart'), '/restart')
        self.assertEqual(_normalize_cmd_token('∆/restart'), '/restart')
        self.assertEqual(_normalize_cmd_token('∆ /restart'), '/restart')
        self.assertEqual(_normalize_cmd_token('!/restart'), '/restart')
        self.assertEqual(_normalize_cmd_token('! /restart'), '/restart')
        self.assertEqual(_normalize_cmd_token('?/restart'), '/restart')
        self.assertEqual(_normalize_cmd_token('? /restart'), '/restart')

    def test_spool_restart_barrier_recognizes_prefixed_restart(self) -> None:
        self.assertTrue(
            _spool_record_is_restart(
                {
                    'kind': 'text',
                    'chat_id': 1,
                    'user_id': 1,
                    'text': '∆ /restart',
                }
            )
        )

    def test_event_restart_barrier_recognizes_prefixed_restart(self) -> None:
        ev = Event(kind='text', chat_id=1, chat_type='private', user_id=1, text='∆/restart')
        self.assertTrue(_event_is_restart(ev))
