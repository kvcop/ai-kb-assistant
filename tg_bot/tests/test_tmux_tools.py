import os
import unittest
from unittest.mock import patch

from tg_bot import tmux_tools


class TestTmuxTools(unittest.TestCase):
    def test_escape_imagemagick_text(self) -> None:
        self.assertEqual(tmux_tools._escape_imagemagick_text('100%'), '100%%')
        self.assertEqual(tmux_tools._escape_imagemagick_text('%a%b%'), '%%a%%b%%')

    def test_sanitize_basename(self) -> None:
        name = tmux_tools._sanitize_basename('  tmux pane %29 / 3:4.1  ')
        self.assertTrue(name)
        self.assertNotIn(' ', name)
        self.assertNotIn('%', name)
        self.assertLessEqual(len(name), 80)

    def test_truncate_for_tg(self) -> None:
        s = 'x' * 100
        out = tmux_tools._truncate_for_tg(s, max_chars=10)
        self.assertEqual(len(out), 10)
        self.assertTrue(out.endswith('…'))

    def test_list_panes_parses_tmux_output(self) -> None:
        raw = '\n'.join(
            [
                '3\t1\tcodex\t1\t%25\t0\t88\t26\tpane one',
                '3\t1\tcodex\t2\t%91\t1\t88\t25\tpane two',
                '2\t0\tmain\t1\t%10\t0\t100\t40\tfirst',
            ]
        )

        with patch.object(tmux_tools, '_run_tmux', return_value=raw):
            panes = tmux_tools.list_panes()

        self.assertEqual([p.session_name for p in panes], ['2', '3', '3'])
        self.assertEqual(panes[0].pane_id, '%10')
        self.assertEqual(panes[1].pane_id, '%25')
        self.assertEqual(panes[2].pane_active, True)
        self.assertEqual(panes[2].pane_height, 25)

    def test_capture_pane_text_defaults_to_visible_height(self) -> None:
        calls: list[list[str]] = []

        def fake_run(args: list[str], *, check: bool = True) -> str:
            calls.append(list(args))
            if args[:2] == ['display-message', '-p']:
                return '26\n'
            if args and args[0] == 'capture-pane':
                self.assertIn('-S', args)
                s_idx = args.index('-S')
                self.assertEqual(args[s_idx + 1], '-26')
                return 'hello\n'
            return ''

        with patch.object(tmux_tools, '_run_tmux', side_effect=fake_run):
            out = tmux_tools.capture_pane_text('%29', lines=None)

        self.assertEqual(out, 'hello\n')
        self.assertTrue(any(c and c[0] == 'capture-pane' for c in calls))

    def test_default_target_prefers_tmux_env_pane(self) -> None:
        with patch.dict(os.environ, {'TMUX': '1'}, clear=False):
            with patch.object(tmux_tools, '_run_tmux', return_value='%77\n'):
                self.assertEqual(tmux_tools._default_target(), '%77')
