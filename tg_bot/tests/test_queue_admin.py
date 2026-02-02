import queue
import unittest

from tg_bot.queue_admin import mutate_queue


class TestQueueAdmin(unittest.TestCase):
    def _mkq(self, items: list[object]) -> queue.Queue[object]:
        q: queue.Queue[object] = queue.Queue()
        for x in items:
            q.put(x)
        return q

    def _snapshot(self, q: queue.Queue[object]) -> list[object]:
        with q.mutex:
            return list(q.queue)

    def test_mutate_queue_delete(self) -> None:
        q = self._mkq([1, 2, 3])
        res = mutate_queue(q, action='del', index=1)
        self.assertTrue(res.get('ok'))
        self.assertTrue(res.get('changed'))
        self.assertEqual(self._snapshot(q), [1, 3])

    def test_mutate_queue_up_noop_on_first(self) -> None:
        q = self._mkq(['a', 'b'])
        res = mutate_queue(q, action='up', index=0)
        self.assertTrue(res.get('ok'))
        self.assertFalse(res.get('changed'))
        self.assertEqual(self._snapshot(q), ['a', 'b'])

    def test_mutate_queue_up_swaps(self) -> None:
        q = self._mkq(['a', 'b', 'c'])
        res = mutate_queue(q, action='up', index=1)
        self.assertTrue(res.get('ok'))
        self.assertTrue(res.get('changed'))
        self.assertEqual(self._snapshot(q), ['b', 'a', 'c'])

    def test_mutate_queue_down_noop_on_last(self) -> None:
        q = self._mkq(['a', 'b'])
        res = mutate_queue(q, action='down', index=1)
        self.assertTrue(res.get('ok'))
        self.assertFalse(res.get('changed'))
        self.assertEqual(self._snapshot(q), ['a', 'b'])

    def test_mutate_queue_down_swaps(self) -> None:
        q = self._mkq(['a', 'b', 'c'])
        res = mutate_queue(q, action='down', index=1)
        self.assertTrue(res.get('ok'))
        self.assertTrue(res.get('changed'))
        self.assertEqual(self._snapshot(q), ['a', 'c', 'b'])

    def test_mutate_queue_invalid_index(self) -> None:
        q = self._mkq([1, 2, 3])
        res = mutate_queue(q, action='del', index=99)
        self.assertFalse(res.get('ok'))
        self.assertEqual(res.get('error'), 'out_of_range')

    def test_mutate_queue_invalid_action(self) -> None:
        q = self._mkq([1, 2, 3])
        res = mutate_queue(q, action='wat', index=1)
        self.assertFalse(res.get('ok'))
        self.assertEqual(res.get('error'), 'bad_action')
