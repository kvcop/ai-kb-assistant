import unittest

from tg_bot.ui_labels import codex_resume_label


class TestUiLabels(unittest.TestCase):
    def test_codex_resume_label_per_chat_when_no_thread(self) -> None:
        self.assertEqual(codex_resume_label(message_thread_id=0), 'per-chat resume')

    def test_codex_resume_label_per_topic_when_thread_present(self) -> None:
        self.assertEqual(codex_resume_label(message_thread_id=123), 'per-topic resume')
