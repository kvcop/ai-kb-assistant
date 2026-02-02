import unittest

from tg_bot import keyboards


class TestQueueUI(unittest.TestCase):
    def test_queue_menu_callback_data(self) -> None:
        kb = keyboards.queue_menu(page=0, pages=3)
        self.assertIsInstance(kb, dict)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)

        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_PAGE_PREFIX) for d in btn_data))
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_EDIT_PREFIX) for d in btn_data))
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_CLEAR_PREFIX) for d in btn_data))
        for d in btn_data:
            if isinstance(d, str):
                self.assertLessEqual(len(d.encode('utf-8')), 64)

    def test_queue_menu_edit_has_item_buttons(self) -> None:
        item_buttons = [
            ('1', f'{keyboards.CB_QUEUE_ITEM_PREFIX}main:0:0'),
            ('2', f'{keyboards.CB_QUEUE_ITEM_PREFIX}main:1:0'),
        ]
        kb = keyboards.queue_menu(page=0, pages=1, edit_active=True, item_buttons=item_buttons)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)
        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_DONE_PREFIX) for d in btn_data))
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_ITEM_PREFIX) for d in btn_data))
        for d in btn_data:
            if isinstance(d, str):
                self.assertLessEqual(len(d.encode('utf-8')), 64)

    def test_queue_item_menu_callback_data(self) -> None:
        kb = keyboards.queue_item_menu(bucket='main', index=1, page=2, edit_active=True)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)
        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_PAGE_PREFIX) for d in btn_data))
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_DONE_PREFIX) for d in btn_data))
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_ACT_PREFIX) for d in btn_data))
        self.assertTrue(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_CLEAR_PREFIX) for d in btn_data))
        for d in btn_data:
            if isinstance(d, str):
                self.assertLessEqual(len(d.encode('utf-8')), 64)

    def test_queue_item_menu_readonly_has_no_actions(self) -> None:
        kb = keyboards.queue_item_menu(bucket='main', index=1, page=2, edit_active=False)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)
        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertFalse(any(isinstance(d, str) and d.startswith(keyboards.CB_QUEUE_ACT_PREFIX) for d in btn_data))

    def test_queue_item_menu_spool_has_delete_in_edit_mode(self) -> None:
        kb = keyboards.queue_item_menu(bucket='spool', index=1, page=2, edit_active=True)
        rows = kb.get('inline_keyboard')
        self.assertIsInstance(rows, list)
        btn_data = [b.get('callback_data') for row in rows for b in (row or []) if isinstance(b, dict)]
        self.assertIn('queue_act:spool:1:del:2', btn_data)
        self.assertFalse(any(isinstance(d, str) and ':up:' in d for d in btn_data))
        self.assertFalse(any(isinstance(d, str) and ':down:' in d for d in btn_data))
        for d in btn_data:
            if isinstance(d, str):
                self.assertLessEqual(len(d.encode('utf-8')), 64)
