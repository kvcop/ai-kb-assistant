import json
import tempfile
import unittest
from pathlib import Path

from tg_bot import keyboards
from tg_bot.spool_admin import delete_spool_item, preview_spool


class TestSpoolAdmin(unittest.TestCase):
    def test_preview_spool_counts_and_labels(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / 'queue.jsonl'
            p.write_text(
                '\n'.join(
                    [
                        json.dumps({'kind': 'text', 'chat_id': 1, 'user_id': 2, 'message_id': 10, 'text': 'hello'}),
                        json.dumps(
                            {
                                'kind': 'callback',
                                'chat_id': 1,
                                'user_id': 2,
                                'message_id': 11,
                                'text': keyboards.CB_CX_PLAN3,
                            }
                        ),
                        '',
                    ]
                ),
                encoding='utf-8',
            )

            out = preview_spool(path=p, max_items=10)
            self.assertEqual(out.get('n'), 2)
            self.assertFalse(bool(out.get('truncated')))
            head = out.get('head')
            self.assertIsInstance(head, list)
            self.assertEqual(len(head), 2)
            self.assertIn('spool text', head[0])
            self.assertIn('hello', head[0])
            self.assertIn('spool callback', head[1])
            self.assertIn('План 3 шага', head[1])

            out2 = preview_spool(path=p, max_items=1)
            self.assertEqual(out2.get('n'), 2)
            head2 = out2.get('head')
            self.assertIsInstance(head2, list)
            self.assertEqual(len(head2), 1)

    def test_delete_spool_item_removes_by_valid_index(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / 'queue.jsonl'
            p.write_text(
                '\n'.join(
                    [
                        json.dumps({'kind': 'text', 'chat_id': 1, 'user_id': 2, 'message_id': 1, 'text': 'a'}),
                        json.dumps({'kind': 'text', 'chat_id': 1, 'user_id': 2, 'message_id': 2, 'text': 'b'}),
                        json.dumps({'kind': 'text', 'chat_id': 1, 'user_id': 2, 'message_id': 3, 'text': 'c'}),
                        '',
                    ]
                ),
                encoding='utf-8',
            )

            res = delete_spool_item(path=p, index=1)
            self.assertTrue(bool(res.get('ok')))
            self.assertTrue(bool(res.get('changed')))
            self.assertEqual(res.get('n'), 2)

            out = preview_spool(path=p, max_items=10)
            self.assertEqual(out.get('n'), 2)
            head = out.get('head')
            self.assertIsInstance(head, list)
            joined = '\n'.join(head)
            self.assertIn('a', joined)
            self.assertIn('c', joined)
            self.assertNotIn('b', joined)

            bad = delete_spool_item(path=p, index=10)
            self.assertFalse(bool(bad.get('ok')))
            self.assertEqual(bad.get('error'), 'out_of_range')

    def test_delete_last_item_unlinks_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / 'queue.jsonl'
            p.write_text(
                json.dumps({'kind': 'text', 'chat_id': 1, 'user_id': 2, 'message_id': 1, 'text': 'a'}) + '\n',
                encoding='utf-8',
            )
            res = delete_spool_item(path=p, index=0)
            self.assertTrue(bool(res.get('ok')))
            self.assertEqual(res.get('n'), 0)
            out = preview_spool(path=p, max_items=10)
            self.assertEqual(out.get('n'), 0)
