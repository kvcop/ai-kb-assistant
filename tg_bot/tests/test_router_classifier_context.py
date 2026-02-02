import unittest

from tg_bot.router import _build_classifier_payload, _reminder_reply_write_hint


class TestRouterClassifierContext(unittest.TestCase):
    def test_build_classifier_payload_includes_reply_and_attachments(self) -> None:
        payload = _build_classifier_payload(
            user_text='перенеси на 17:00',
            reply_to={'text': '⏰ 15:00: ChatGPT Pro: https://example.com/x'},
            attachments=[{'name': 'file.txt', 'path': 'tg_uploads/file.txt', 'kind': 'photo'}],
        )
        self.assertIn('перенеси на 17:00', payload)
        self.assertIn('Context:', payload)
        self.assertIn('reply_to:', payload)
        self.assertIn('⏰ 15:00', payload)
        self.assertIn('<url>', payload)
        self.assertIn('attachments:', payload)
        self.assertIn('file.txt', payload)

    def test_reminder_reply_write_hint_true(self) -> None:
        reply_to = {'text': '⏰ 15:00: Something'}
        self.assertTrue(_reminder_reply_write_hint(user_text='перенеси на 17:00', reply_to=reply_to))
        self.assertTrue(_reminder_reply_write_hint(user_text='17:00', reply_to=reply_to))
        self.assertTrue(_reminder_reply_write_hint(user_text='на 17', reply_to=reply_to))

    def test_reminder_reply_write_hint_false(self) -> None:
        reply_to = {'text': '⏰ 15:00: Something'}
        self.assertFalse(_reminder_reply_write_hint(user_text='спасибо', reply_to=reply_to))
        self.assertFalse(_reminder_reply_write_hint(user_text='перенеси на 17:00', reply_to={'text': 'not a reminder'}))
