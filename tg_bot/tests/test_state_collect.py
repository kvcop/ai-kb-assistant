import json
import tempfile
import unittest
from pathlib import Path

from tg_bot.state import BotState


class TestCollectStateSlice(unittest.TestCase):
    def test_collect_slice_load_and_compatibility_with_legacy_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text(
                json.dumps(
                    {
                        'tg_offset': 10,
                        'collect_active': {'1:2': {'id': 'a'}},
                        'collect_pending': {'1:2': [{'id': 'p1'}, {'id': 'p2'}]},
                        'collect_deferred': {'1:2': [{'id': 'd1'}]},
                    }
                ),
                encoding='utf-8',
            )

            st = BotState(path=state_path)
            st.load()
            self.assertEqual(st.collect_active.get('1:2'), {'id': 'a'})
            self.assertEqual(st.collect_pending.get('1:2'), [{'id': 'p1'}, {'id': 'p2'}])
            self.assertEqual(st.collect_deferred.get('1:2'), [{'id': 'd1'}])
            self.assertEqual(st.status(chat_id=1, message_thread_id=2), 'active')

            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            self.assertEqual(st.collect_active, {})
            self.assertEqual(st.collect_pending, {})
            self.assertEqual(st.collect_deferred, {})
            self.assertEqual(st.status(chat_id=1, message_thread_id=2), 'idle')

    def test_collect_api_append_start_complete_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            self.assertEqual(st.status(chat_id=10, message_thread_id=1), 'idle')

            st.append(chat_id=10, message_thread_id=1, item={'id': 'a'})
            st.append(chat_id=10, message_thread_id=1, item={'id': 'b'})
            st.append(chat_id=10, message_thread_id=1, item={'id': 'c'})
            self.assertEqual(st.status(chat_id=10, message_thread_id=1), 'pending')
            self.assertEqual([x.get('id') for x in st.collect_pending.get('10:1')], ['a', 'b', 'c'])

            first = st.start(chat_id=10, message_thread_id=1)
            self.assertIsNotNone(first)
            self.assertEqual(first.get('id'), 'a')
            self.assertEqual(st.status(chat_id=10, message_thread_id=1), 'active')
            self.assertEqual(st.collect_active.get('10:1'), {'id': 'a'})

            completed = st.complete(chat_id=10, message_thread_id=1)
            self.assertIsNotNone(completed)
            self.assertEqual(completed.get('id'), 'a')
            self.assertEqual(st.status(chat_id=10, message_thread_id=1), 'pending')
            self.assertEqual(st.collect_deferred.get('10:1'), [{'id': 'a'}])
            self.assertEqual(st.collect_pending.get('10:1'), [{'id': 'b'}, {'id': 'c'}])

            second = st.start(chat_id=10, message_thread_id=1)
            self.assertIsNotNone(second)
            self.assertEqual(second.get('id'), 'b')
            self.assertEqual(st.status(chat_id=10, message_thread_id=1), 'active')

            canceled = st.cancel(chat_id=10, message_thread_id=1)
            self.assertIsNotNone(canceled)
            self.assertEqual(canceled.get('id'), 'b')
            self.assertEqual(st.status(chat_id=10, message_thread_id=1), 'pending')
            self.assertEqual(st.collect_pending.get('10:1'), [{'id': 'c'}])
            self.assertEqual(st.collect_deferred.get('10:1'), [{'id': 'a'}])

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.status(chat_id=10, message_thread_id=1), 'pending')
            self.assertEqual(st2.collect_pending.get('10:1'), [{'id': 'c'}])
            self.assertEqual(st2.collect_deferred.get('10:1'), [{'id': 'a'}])

    def test_collect_api_isolated_by_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.append(chat_id=1, message_thread_id=0, item={'id': 'root'})
            st.append(chat_id=1, message_thread_id=1, item={'id': 'thread'})

            self.assertEqual(st.status(chat_id=1, message_thread_id=0), 'pending')
            self.assertEqual(st.status(chat_id=1, message_thread_id=1), 'pending')

            first = st.start(chat_id=1, message_thread_id=0)
            self.assertIsNotNone(first)
            self.assertEqual(first.get('id'), 'root')
            self.assertEqual(st.status(chat_id=1, message_thread_id=0), 'active')
            self.assertEqual(st.status(chat_id=1, message_thread_id=1), 'pending')

    def test_collect_start_cleans_dirty_pending_to_idle_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.collect_pending['2:3'] = [1, 2, 3]
            st.save()
            self.assertEqual(st.status(chat_id=2, message_thread_id=3), 'pending')
            self.assertEqual(st.collect_start(chat_id=2, message_thread_id=3), None)
            self.assertEqual(st.status(chat_id=2, message_thread_id=3), 'idle')
            self.assertNotIn('2:3', st.collect_pending)

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.status(chat_id=2, message_thread_id=3), 'idle')
            self.assertNotIn('2:3', st2.collect_pending)

    def test_collect_start_cleans_dirty_pending_to_idle_and_calls_save(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.collect_pending['9:10'] = [1, 2, 3]

            save_calls: list[bool] = []

            def fake_save() -> None:
                acquired = st.lock.acquire(blocking=False)
                if acquired:
                    st.lock.release()
                save_calls.append(acquired)

            st.save = fake_save

            self.assertIsNone(st.collect_start(chat_id=9, message_thread_id=10))
            self.assertEqual(st.status(chat_id=9, message_thread_id=10), 'idle')
            self.assertNotIn('9:10', st.collect_pending)
            self.assertEqual(save_calls, [True])

    def test_set_collect_packet_decision_invalid_status_removes_entry_without_holding_lock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            chat_id = 3
            message_thread_id = 4
            packet_id = 'packet-1'

            st.set_collect_packet_decision(
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                packet_id=packet_id,
                status='pending',
                reasons=['oversize'],
            )
            self.assertIsNotNone(st.collect_packet_decision(chat_id=chat_id, message_thread_id=message_thread_id, packet_id=packet_id))

            save_lock_free: list[bool] = []

            def fake_save() -> None:
                acquired = st.lock.acquire(blocking=False)
                if acquired:
                    st.lock.release()
                save_lock_free.append(acquired)

            st.save = fake_save

            st.set_collect_packet_decision(
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                packet_id=packet_id,
                status=None,
            )

            self.assertIsNone(st.collect_packet_decision(chat_id=chat_id, message_thread_id=message_thread_id, packet_id=packet_id))
            self.assertNotIn('3:4', st.collect_packet_decisions_by_scope)
            self.assertEqual(save_lock_free, [True])
