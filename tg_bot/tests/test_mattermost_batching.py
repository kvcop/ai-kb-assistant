import unittest

from tg_bot.mattermost_watch import _mm_collect_posts_for_batch


class TestMattermostBatching(unittest.TestCase):
    def test_triggers_on_oldest_due_and_includes_newer(self) -> None:
        posts = [
            {'id': 'p1', 'user_id': 'u2', 'create_at': 1000, 'delete_at': 0, 'type': '', 'message': 'a'},
            {'id': 'p2', 'user_id': 'u2', 'create_at': 1010, 'delete_at': 0, 'type': '', 'message': 'b'},
            {'id': 'p3', 'user_id': 'u2', 'create_at': 1020, 'delete_at': 0, 'type': '', 'message': 'c'},
        ]

        items, due_count, max_due_ts, max_ts = _mm_collect_posts_for_batch(
            posts, me_id='me', sent_cutoff=0, cutoff_ms=1015
        )
        self.assertEqual([ts for ts, _ in items], [1000, 1010, 1020])
        self.assertEqual(due_count, 2)
        self.assertEqual(max_due_ts, 1010)
        self.assertEqual(max_ts, 1020)

    def test_does_not_trigger_when_all_posts_newer_than_cutoff(self) -> None:
        posts = [
            {'id': 'p1', 'user_id': 'u2', 'create_at': 2000, 'delete_at': 0, 'type': '', 'message': 'a'},
            {'id': 'p2', 'user_id': 'u2', 'create_at': 2010, 'delete_at': 0, 'type': '', 'message': 'b'},
        ]

        items, due_count, max_due_ts, max_ts = _mm_collect_posts_for_batch(
            posts, me_id='me', sent_cutoff=0, cutoff_ms=1500
        )
        self.assertEqual([ts for ts, _ in items], [2000, 2010])
        self.assertEqual(due_count, 0)
        self.assertEqual(max_due_ts, 0)
        self.assertEqual(max_ts, 2010)

    def test_filters_self_system_deleted_empty_and_sent_cutoff(self) -> None:
        posts = [
            # too old: already sent
            {'id': 'old', 'user_id': 'u2', 'create_at': 10, 'delete_at': 0, 'type': '', 'message': 'old'},
            # self
            {'id': 'self', 'user_id': 'me', 'create_at': 1000, 'delete_at': 0, 'type': '', 'message': 'self'},
            # deleted
            {'id': 'del', 'user_id': 'u2', 'create_at': 1100, 'delete_at': 1, 'type': '', 'message': 'del'},
            # system
            {
                'id': 'sys',
                'user_id': 'u2',
                'create_at': 1200,
                'delete_at': 0,
                'type': 'system_join_channel',
                'message': 'sys',
            },
            # empty msg
            {'id': 'empty', 'user_id': 'u2', 'create_at': 1300, 'delete_at': 0, 'type': '', 'message': ''},
            # ok
            {'id': 'ok', 'user_id': 'u2', 'create_at': 1400, 'delete_at': 0, 'type': '', 'message': 'ok'},
        ]

        items, due_count, max_due_ts, max_ts = _mm_collect_posts_for_batch(
            posts, me_id='me', sent_cutoff=999, cutoff_ms=2000
        )
        self.assertEqual([ts for ts, _ in items], [1400])
        self.assertEqual(due_count, 1)
        self.assertEqual(max_due_ts, 1400)
        self.assertEqual(max_ts, 1400)
