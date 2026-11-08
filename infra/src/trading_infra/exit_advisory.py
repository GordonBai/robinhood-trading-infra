"""Bounded, out-of-process exit advice.

The owner loop must not run strategy code: a plugin callback that hangs, raises
or needs a lock would stop reconciliation and protective management for every
instance. Each advice is therefore computed in its own short-lived process with
a hard timeout, and the owner only ever sees a small data result.

The service is deliberately dumb about identity: it returns what the plugin
said, tagged with the pid that computed it. Binding the advice to a trade, a
quantity and a validity window is the execution authority's job.
"""
import json
import multiprocessing
import queue
import time


def _resolve(spec):
    module_name, _, attribute = spec.partition(':')
    if not module_name or not attribute:
        raise ValueError('EXIT_ADVISOR_SPEC_INVALID')
    identity = None
    if '@' in attribute:
        attribute, _, identity = attribute.partition('@')
        if not identity or len(identity) != 64:
            raise ValueError('EXIT_ADVISOR_IDENTITY_INVALID')
    if identity is not None:
        from trading_infra.plugin_protocol import distribution_identity_for_module
        installed = distribution_identity_for_module(module_name)
        if installed.digest != identity:
            raise ValueError('EXIT_ADVISOR_IDENTITY_MISMATCH')
    module = __import__(module_name, fromlist=[attribute])
    return getattr(module, attribute)


def _child(spec, view, output):
    try:
        rule = _resolve(spec)
        advice = rule(view)
        output.put({'ok': True, 'advice': json.loads(json.dumps(advice, default=str)),
                    'pid': multiprocessing.current_process().pid})
    except Exception as exc:  # reported, never re-raised into the owner
        output.put({'ok': False, 'reason': type(exc).__name__,
                    'pid': multiprocessing.current_process().pid})


class ExitAdvisoryService:
    """Compute one advice per position view, each in a bounded child process."""

    def __init__(self, specs, *, timeout_seconds=1.0, advice_ttl_seconds=20,
                 max_inflight=4):
        if not isinstance(specs, dict) or not specs:
            raise ValueError('EXIT_ADVISOR_SPECS_REQUIRED')
        if timeout_seconds <= 0:
            raise ValueError('EXIT_ADVISOR_TIMEOUT_INVALID')
        self.specs = dict(specs)
        self.timeout_seconds = timeout_seconds
        self.advice_ttl_seconds = advice_ttl_seconds
        self.max_inflight = max_inflight
        self._context = multiprocessing.get_context('spawn')
        self._pending = {}

    def advise_one(self, view):
        spec = self.specs.get(view.get('plugin_id'))
        if spec is None:
            return {'error': 'EXIT_ADVISOR_NOT_REGISTERED'}
        output = self._context.Queue()
        process = self._context.Process(target=_child, args=(spec, dict(view), output),
                                        daemon=True)
        process.start()
        process.join(self.timeout_seconds)
        if process.is_alive():
            process.terminate()
            process.join(0.2)
            if process.is_alive():
                process.kill()
                process.join(0.2)
            return {'error': 'EXIT_ADVISOR_TIMEOUT'}
        try:
            result = output.get_nowait()
        except Exception:
            return {'error': 'EXIT_ADVISOR_NO_RESULT'}
        finally:
            output.close()
        if not result.get('ok'):
            return {'error': 'EXIT_ADVISOR_FAILED:' + str(result.get('reason')),
                    'pid': result.get('pid')}
        advice = dict(result['advice'])
        advice['advisor_pid'] = result['pid']
        # Binding is infrastructure policy: the plugin supplies the judgement,
        # not the identity or the validity window it will be applied under.
        from trading_contracts.values import timestamp
        from datetime import timedelta
        as_of = timestamp(view['as_of'])
        advice['trade_id'] = view['trade_id']
        advice['as_of'] = as_of.isoformat()
        advice['valid_until'] = (as_of + timedelta(
            seconds=self.advice_ttl_seconds)).isoformat()
        advice['position_version'] = view['position_version']
        advice['plugin_id'] = view['plugin_id']
        return advice

    def submit(self, views):
        """Start at most ``max_inflight`` advisors; never waits for any."""
        started = 0
        for view in views:
            trade_id = view.get('trade_id')
            if not trade_id or trade_id in self._pending:
                continue
            if len(self._pending) >= self.max_inflight:
                # The bound is per process count, not per position: a hung
                # plugin cannot add latency with every extra position.
                break
            spec = self.specs.get(view.get('plugin_id'))
            if spec is None:
                continue
            output = self._context.Queue()
            process = self._context.Process(target=_child,
                                            args=(spec, dict(view), output),
                                            daemon=True)
            process.start()
            self._pending[trade_id] = (process, output, dict(view),
                                       time.monotonic() + self.timeout_seconds)
            started += 1
        return started

    def collect(self):
        """Non-blocking harvest of finished, failed or timed-out advisors."""
        results = {}
        now = time.monotonic()
        for trade_id, (process, output, view, deadline) in list(self._pending.items()):
            if process.is_alive():
                if now < deadline:
                    continue
                process.terminate()
                process.join(0.05)
                if process.is_alive():
                    process.kill()
                    process.join(0.05)
                results[trade_id] = {'error': 'EXIT_ADVISOR_TIMEOUT',
                                     'pid': process.pid}
            else:
                try:
                    # The child may have exited while its queue feeder is
                    # still flushing. A short bounded read observes that
                    # completion; it is not a timeout extension.
                    result = output.get(timeout=0.05)
                except queue.Empty:
                    result = {'ok': False, 'reason': 'NO_RESULT'}
                except Exception:
                    result = {'ok': False, 'reason': 'NO_RESULT'}
                if not result.get('ok'):
                    results[trade_id] = {
                        'error': 'EXIT_ADVISOR_FAILED:' + str(result.get('reason')),
                        'pid': result.get('pid')}
                else:
                    advice = dict(result['advice'])
                    advice['advisor_pid'] = result['pid']
                    advice['trade_id'] = view['trade_id']
                    advice['as_of'] = view['as_of']
                    from datetime import timedelta
                    from trading_contracts.values import timestamp
                    as_of = timestamp(view['as_of'])
                    advice['valid_until'] = (as_of + timedelta(
                        seconds=self.advice_ttl_seconds)).isoformat()
                    advice['position_version'] = view['position_version']
                    advice['plugin_id'] = view['plugin_id']
                    results[trade_id] = advice
            output.close()
            self._pending.pop(trade_id, None)
        return results

    def advise_one(self, view):
        """Blocking compatibility helper for one view (tests/diagnostics)."""
        self.submit([view])
        deadline = time.monotonic() + self.timeout_seconds + 0.5
        while time.monotonic() < deadline:
            results = self.collect()
            if view['trade_id'] in results:
                return results[view['trade_id']]
            time.sleep(0.01)
        return {'error': 'EXIT_ADVISOR_TIMEOUT'}

