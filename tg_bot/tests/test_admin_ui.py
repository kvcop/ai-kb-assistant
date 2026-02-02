import unittest

from tg_bot import keyboards


class TestAdminUI(unittest.TestCase):
    def test_admin_menu_callback_data(self) -> None:
        kb = keyboards.admin_menu(queue_page=0)
        self.assertIsInstance(kb, dict)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)
        self.assertGreaterEqual(len(rows), 3)

        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertIn(keyboards.CB_SETTINGS, btn_data)
        self.assertIn(keyboards.CB_ADMIN_DOCTOR, btn_data)
        self.assertIn(keyboards.CB_ADMIN_STATS, btn_data)
        self.assertIn(keyboards.CB_ADMIN_DROP_QUEUE, btn_data)
        self.assertIn(keyboards.CB_ADMIN_DROP_ALL, btn_data)
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_PAGE_PREFIX) for d in btn_data))

        for d in btn_data:
            if isinstance(d, str):
                self.assertLessEqual(len(d.encode('utf-8')), 64)
