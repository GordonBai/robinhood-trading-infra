"""Bounded single-operation probe for conformance. Trusted code, not a sandbox."""
import argparse
import base64
import json
import sys

from trading_contracts.codec import decode_input, encode_decision
from trading_infra.plugin_protocol import distribution_identity_for_module, load_plugin
from trading_infra.strategy_host import evaluate_strategy


def _emit(value):
    sys.stdout.write(json.dumps(value, sort_keys=True, default=str) + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plugin', required=True)
    parser.add_argument('--config', default='{}')
    parser.add_argument('--op', required=True,
                        choices=['describe', 'create', 'decide', 'exit'])
    parser.add_argument('--request-b64')
    parser.add_argument('--view-b64')
    args = parser.parse_args()
    try:
        loaded = load_plugin(args.plugin)
        config = json.loads(args.config)
        plugin_config = dict(config)
        plugin_config.pop('cost_buffer_per_share', None)
        if args.op == 'describe':
            _emit({'ok': True, 'descriptor': loaded.descriptor.as_dict(),
                   'distribution_digest': loaded.distribution.digest,
                   'distribution': loaded.distribution.distribution,
                   'distribution_version': loaded.distribution.distribution_version})
        elif args.op == 'create':
            loaded.create(plugin_config)
            _emit({'ok': True})
        elif args.op == 'decide':
            if not args.request_b64:
                raise ValueError('PLUGIN_PROBE_REQUEST_REQUIRED')
            request = decode_input(base64.b64decode(args.request_b64))
            plugin = loaded.create(plugin_config)
            decision = evaluate_strategy(plugin, request)
            _emit({'ok': True,
                   'decision_b64': base64.b64encode(
                       encode_decision(decision)).decode('ascii')})
        elif args.op == 'exit':
            if not args.view_b64:
                raise ValueError('PLUGIN_PROBE_VIEW_REQUIRED')
            module_name, _, function = loaded.descriptor.exit_rule.partition(':')
            identity = distribution_identity_for_module(module_name)
            if identity.digest != loaded.distribution.digest:
                raise ValueError('PLUGIN_EXIT_IDENTITY_MISMATCH')
            rule = getattr(__import__(module_name, fromlist=[function]), function)
            _emit({'ok': True, 'result': rule(json.loads(
                base64.b64decode(args.view_b64)))})
        return 0
    except Exception as exc:
        _emit({'ok': False, 'error': str(exc) or type(exc).__name__})
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
