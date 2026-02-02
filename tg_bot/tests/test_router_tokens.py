import unittest

from tg_bot.router import _strip_fastthink_token, _strip_ultrathink_token


class TestThinkTokens(unittest.TestCase):
    def test_ultrathink_detects_and_strips(self) -> None:
        out, enabled = _strip_ultrathink_token('hi ultrathink there')
        self.assertTrue(enabled)
        self.assertNotIn('ultrathink', out.lower())
        self.assertTrue(out.strip())

    def test_ultrathink_word_boundary(self) -> None:
        out, enabled = _strip_ultrathink_token('ultrathinking')
        self.assertFalse(enabled)
        self.assertEqual(out, 'ultrathinking')

    def test_fastthink_detects_and_strips(self) -> None:
        out, enabled = _strip_fastthink_token('hi fastthink there')
        self.assertTrue(enabled)
        self.assertNotIn('fastthink', out.lower())
        self.assertTrue(out.strip())

    def test_fastthink_word_boundary(self) -> None:
        out, enabled = _strip_fastthink_token('fastthinking')
        self.assertFalse(enabled)
        self.assertEqual(out, 'fastthinking')

    def test_both_tokens_can_be_stripped(self) -> None:
        s, ultra = _strip_ultrathink_token('a ultrathink b fastthink c')
        s2, fast = _strip_fastthink_token(s)
        self.assertTrue(ultra)
        self.assertTrue(fast)
        self.assertNotIn('ultrathink', s2.lower())
        self.assertNotIn('fastthink', s2.lower())
