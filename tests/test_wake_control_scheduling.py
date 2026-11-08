"""Wake-result readiness and expiry schedule owner ticks without Agent polling."""
import queue
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from martin_luk.live_session import wait_for_result_or_tick


def task(running=False):
    return SimpleNamespace(thread=object() if running else None,mailbox=queue.Queue())


class WakeControlSchedulingTests(TestCase):
    def test_ready_detached_wake_result_interrupts_full_tick_without_consuming_mailbox(self):
        wake=task(True);wake.mailbox.put((True,{'receipt_id':'queued-only'}))
        bridge=SimpleNamespace(task=task(),wake_dispatcher=SimpleNamespace(task=wake,next_deadline_monotonic=101))
        with patch('martin_agentic.live_session.time.monotonic',return_value=100), \
                patch('martin_agentic.live_session.time.sleep') as sleep:
            wait_for_result_or_tick(task(),bridge,15)
        sleep.assert_not_called();self.assertEqual(wake.mailbox.qsize(),1)

    def test_owner_returns_at_wake_deadline_even_while_callback_remains_hung(self):
        now=[100.0];pauses=[]
        def sleep(seconds):pauses.append(seconds);now[0]+=seconds
        bridge=SimpleNamespace(task=task(),wake_dispatcher=SimpleNamespace(task=task(True),next_deadline_monotonic=100.35))
        with patch('martin_agentic.live_session.time.monotonic',side_effect=lambda:now[0]), \
                patch('martin_agentic.live_session.time.sleep',side_effect=sleep):
            wait_for_result_or_tick(task(True),bridge,15)
        self.assertAlmostEqual(sum(pauses),.35);self.assertLessEqual(max(pauses),.1)

    def test_recorded_timeout_with_retained_hung_worker_uses_normal_tick_not_hot_loop(self):
        now=[100.0];pauses=[]
        def sleep(seconds):pauses.append(seconds);now[0]+=seconds
        bridge=SimpleNamespace(task=task(),wake_dispatcher=SimpleNamespace(task=task(True),next_deadline_monotonic=None))
        with patch('martin_agentic.live_session.time.monotonic',side_effect=lambda:now[0]), \
                patch('martin_agentic.live_session.time.sleep',side_effect=sleep):
            wait_for_result_or_tick(task(),bridge,.5)
        self.assertAlmostEqual(sum(pauses),.5);self.assertLessEqual(len(pauses),6)
