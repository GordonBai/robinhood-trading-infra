"""A control loop that advances several instances without merging their faults."""
from datetime import datetime, timezone
from unittest import TestCase

from trading_infra.instance_supervisor import InstanceSupervisor

NOW = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


class Runtime:
    def __init__(self, instance_id, *, fail=False, pending=1, started=None):
        self.instance_id = instance_id
        self.fail = fail
        self.pending = pending
        self.started = started or ('evt-' + instance_id)
        self.advanced = 0
        self.closed = False

    def advance(self):
        self.advanced += 1
        if self.fail:
            raise RuntimeError('fixture fault')
        return {'completed': [], 'rejected': [{'event_id': 'x', 'reason': 'STALE'}],
                'errors': [{'event_id': 'x', 'reason': 'STALE'}],
                'started': self.started, 'pending': self.pending, 'busy': self.pending > 0}

    def status(self):
        return {'pending': self.pending, 'config_fingerprint': 'c-' + self.instance_id,
                'source_fingerprint': 's' * 64, 'committed': 0}

    def enqueue(self, event):
        return {'instance_id': self.instance_id, 'event_id': event['id']}

    def close(self):
        self.closed = True


class SupervisorTests(TestCase):
    def test_every_instance_advances_exactly_once_per_tick(self):
        alpha, beta = Runtime('alpha'), Runtime('beta')
        supervisor = InstanceSupervisor([alpha, beta])
        report = supervisor.advance()
        self.assertEqual(alpha.advanced, 1)
        self.assertEqual(beta.advanced, 1)
        self.assertEqual(sorted(report['instances']), ['alpha', 'beta'])
        self.assertEqual(report['pending'], 2)

    def test_order_is_deterministic_regardless_of_input_order(self):
        supervisor = InstanceSupervisor([Runtime('beta'), Runtime('alpha')])
        self.assertEqual(supervisor.instance_ids(), ['alpha', 'beta'])

    def test_one_faulting_instance_does_not_block_the_others(self):
        broken, healthy = Runtime('alpha', fail=True), Runtime('beta')
        supervisor = InstanceSupervisor([broken, healthy])
        report = supervisor.advance()
        self.assertFalse(report['instances']['alpha']['advanced'])
        self.assertIn({'instance_id': 'alpha', 'reason': 'RuntimeError'}, report['errors'])
        # The exception path and the normal-rejection path both surface here.
        self.assertIn({'instance_id': 'beta', 'reason': 'STALE',
                       'scope': 'STRATEGY_INSTANCE'}, report['errors'])
        self.assertTrue(report['instances']['beta']['advanced'])
        self.assertEqual(healthy.advanced, 1)

    def test_a_normal_rejection_reaches_the_top_level_with_its_instance(self):
        report = InstanceSupervisor([Runtime('alpha')]).advance()
        self.assertEqual(report['errors'],
                         [{'instance_id': 'alpha', 'reason': 'STALE',
                           'scope': 'STRATEGY_INSTANCE'}])

    def test_rejections_are_counted_per_instance_not_pooled(self):
        report = InstanceSupervisor([Runtime('alpha'), Runtime('beta')]).advance()
        self.assertEqual(report['instances']['alpha']['rejected'], 1)
        self.assertEqual(report['instances']['beta']['rejected'], 1)

    def test_duplicate_instance_ids_are_refused(self):
        with self.assertRaises(ValueError) as ctx:
            InstanceSupervisor([Runtime('alpha'), Runtime('alpha')])
        self.assertEqual(str(ctx.exception), 'INSTANCE_SUPERVISOR_DUPLICATE_INSTANCE')

    def test_an_empty_loop_is_refused(self):
        with self.assertRaises(ValueError):
            InstanceSupervisor([])

    def test_unknown_instance_is_refused(self):
        supervisor = InstanceSupervisor([Runtime('alpha')])
        with self.assertRaises(ValueError) as ctx:
            supervisor.enqueue('beta', {'id': 'e'})
        self.assertEqual(str(ctx.exception), 'INSTANCE_SUPERVISOR_UNKNOWN_INSTANCE')

    def test_enqueue_routes_to_the_named_instance_only(self):
        supervisor = InstanceSupervisor([Runtime('alpha'), Runtime('beta')])
        self.assertEqual(supervisor.enqueue('beta', {'id': 'e1'})['instance_id'], 'beta')

    def test_close_is_idempotent_and_closes_every_runtime(self):
        alpha, beta = Runtime('alpha'), Runtime('beta')
        supervisor = InstanceSupervisor([alpha, beta])
        result = supervisor.close()
        self.assertEqual(result, {'alpha': {'closed': True}, 'beta': {'closed': True}})
        self.assertTrue(alpha.closed and beta.closed)
        self.assertEqual(supervisor.close(), {'closed': True})
        with self.assertRaises(ValueError) as ctx:
            supervisor.advance()
        self.assertEqual(str(ctx.exception), 'INSTANCE_SUPERVISOR_CLOSED')

    def test_state_digest_tracks_the_running_set(self):
        first = InstanceSupervisor([Runtime('alpha'), Runtime('beta')]).state_digest()
        self.assertEqual(first, InstanceSupervisor([Runtime('beta'),
                                                    Runtime('alpha')]).state_digest())
        self.assertNotEqual(first, InstanceSupervisor([Runtime('alpha')]).state_digest())

    def test_status_is_per_instance(self):
        status = InstanceSupervisor([Runtime('alpha'), Runtime('beta')]).status()
        self.assertEqual(status['alpha']['config_fingerprint'], 'c-alpha')
        self.assertEqual(status['beta']['config_fingerprint'], 'c-beta')
