import unittest

from tg_bot.app import _tg_msg_is_forum_topic_created


class TestForumTopicCreated(unittest.TestCase):
    def test_detects_forum_topic_created(self) -> None:
        msg = {
            'message_id': 1,
            'forum_topic_created': {'name': 'Topic', 'icon_color': 123},
        }
        self.assertTrue(_tg_msg_is_forum_topic_created(msg))

    def test_ignores_regular_text_message(self) -> None:
        msg = {'message_id': 2, 'text': 'hi'}
        self.assertFalse(_tg_msg_is_forum_topic_created(msg))

    def test_ignores_missing_payload(self) -> None:
        msg = {'message_id': 3, 'forum_topic_created': None}
        self.assertFalse(_tg_msg_is_forum_topic_created(msg))
