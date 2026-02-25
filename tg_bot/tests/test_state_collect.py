import json
import tempfile
import unittest
import time
from unittest.mock import patch
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

    def test_sleep_until_persists_per_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            now = time.time()
            state_path.write_text(
                json.dumps(
                    {'sleep_until_by_scope': {'1:7': now + 3600.0, 'bad': 'x', 'bad:bad': 'not-ts'}}
                ),
                encoding='utf-8',
            )

            st = BotState(path=state_path)
            st.load()
            self.assertGreater(st.sleep_until(chat_id=1, message_thread_id=7), now)
            self.assertEqual(st.sleep_until(chat_id=1, message_thread_id=8), 0.0)
            st.set_sleep_until(chat_id=1, message_thread_id=8, until_ts=now + 4200.0)
            self.assertEqual(st.sleep_until(chat_id=1, message_thread_id=8), now + 4200.0)

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.sleep_until(chat_id=1, message_thread_id=8), now + 4200.0)

    def test_sleep_until_expires_and_clears(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()

            st.set_sleep_until(chat_id=5, message_thread_id=9, until_ts=1234.0)
            with patch('tg_bot.state._now_ts', return_value=2000.0):
                self.assertEqual(st.sleep_until(chat_id=5, message_thread_id=9), 0.0)

            self.assertEqual(st.sleep_until(chat_id=5, message_thread_id=9), 0.0)

    def test_sleep_until_clear_persists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            until_ts = time.time() + 300.0
            st.set_sleep_until(chat_id=7, message_thread_id=1, until_ts=until_ts)
            self.assertEqual(st.sleep_until(chat_id=7, message_thread_id=1), until_ts)

            st.clear_sleep(chat_id=7, message_thread_id=1)
            self.assertEqual(st.sleep_until(chat_id=7, message_thread_id=1), 0.0)

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.sleep_until(chat_id=7, message_thread_id=1), 0.0)

    def test_last_codex_profile_state_persists_and_isolated_by_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.set_last_codex_profile_state(
                chat_id=10,
                message_thread_id=0,
                mode='read',
                model='gpt-root',
                reasoning='medium',
            )
            st.set_last_codex_profile_state(
                chat_id=10,
                message_thread_id=11,
                mode='write',
                model='gpt-thread',
                reasoning='high',
            )

            self.assertEqual(st.last_codex_profile_state_for(chat_id=10, message_thread_id=0), ('read', 'gpt-root', 'medium'))
            self.assertEqual(st.last_codex_profile_state_for(chat_id=10, message_thread_id=11), ('write', 'gpt-thread', 'high'))

            st.set_last_codex_profile_state(
                chat_id=10,
                message_thread_id=11,
                mode='read',
                reasoning='low',
            )
            self.assertEqual(st.last_codex_profile_state_for(chat_id=10, message_thread_id=11), ('read', 'gpt-thread', 'low'))

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.last_codex_profile_state_for(chat_id=10, message_thread_id=0), ('read', 'gpt-root', 'medium'))
            self.assertEqual(st2.last_codex_profile_state_for(chat_id=10, message_thread_id=11), ('read', 'gpt-thread', 'low'))

    def test_root_model_inherits_to_topic_and_topic_default_clears_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.set_last_codex_profile_state(
                chat_id=10,
                message_thread_id=0,
                mode='read',
                model='gpt-root',
                reasoning='medium',
            )
            st.set_last_codex_profile_state(
                chat_id=10,
                message_thread_id=11,
                mode='read',
                model='gpt-topic',
                reasoning='high',
            )
            self.assertEqual(st.last_codex_model_for(chat_id=10, message_thread_id=11), 'gpt-topic')

            st.set_last_codex_profile_state(chat_id=10, message_thread_id=11, model='')
            self.assertEqual(st.last_codex_profile_state_for(chat_id=10, message_thread_id=11), ('read', 'gpt-root', 'high'))
            self.assertEqual(st.last_codex_model_for(chat_id=10, message_thread_id=11), 'gpt-root')
            self.assertEqual(st.last_codex_model_for(chat_id=10, message_thread_id=0), 'gpt-root')

    def test_codex_profile_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()

            self.assertEqual(st.last_codex_mode_for(chat_id=10, message_thread_id=11), 'read')
            self.assertEqual(st.last_codex_reasoning_for(chat_id=10, message_thread_id=11), 'medium')
            self.assertEqual(st.last_codex_model_for(chat_id=10, message_thread_id=11), '')
