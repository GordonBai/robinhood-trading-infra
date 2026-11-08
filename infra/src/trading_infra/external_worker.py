"""Bounded warm worker for one installed external plugin. No broker access."""
import argparse
import json
import struct
import sys

from trading_contracts.codec import MAX_BYTES, decode_input, encode_decision
from trading_contracts.instance import config_version
from trading_infra.plugin_protocol import load_plugin
from trading_infra.strategy_host import evaluate_strategy


def _read_exact(stream, size, *, allow_eof=False):
    result = bytearray()
    while len(result) < size:
        part = stream.read(size - len(result))
        if not part:
            if allow_eof and not result:
                return None
            raise ValueError('STRATEGY_WORKER_TRUNCATED_FRAME')
        result.extend(part)
    return bytes(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plugin', required=True)
    parser.add_argument('--plugin-digest', required=True)
    parser.add_argument('--plugin-identity', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--source-fingerprint')
    args = parser.parse_args()
    try:
        loaded = load_plugin(args.plugin)
        if loaded.distribution.digest != args.plugin_digest or loaded.distribution.digest != args.plugin_identity:
            raise ValueError('PLUGIN_IDENTITY_MISMATCH')
        config = json.loads(args.config)
        plugin_config = dict(config)
        plugin_config.pop('cost_buffer_per_share', None)
        plugin = loaded.create(plugin_config)
        expected_config = config_version(args.plugin, loaded.descriptor.version, config, loaded.distribution.digest)
        # The worker validates the plugin distribution digest and config
        # binding. The owning host validates its own source identity before
        # dispatching; trading_infra must not depend on martin_agentic.
        while True:
            header = _read_exact(sys.stdin.buffer, 4, allow_eof=True)
            if header is None:
                return 0
            size = struct.unpack('!I', header)[0]
            if not 0 < size <= MAX_BYTES:
                raise ValueError('STRATEGY_WORKER_FRAME_LIMIT')
            request = decode_input(_read_exact(sys.stdin.buffer, size))
            if request.config_version != expected_config:
                raise ValueError('STRATEGY_WORKER_CONFIG_MISMATCH')
            response = encode_decision(evaluate_strategy(plugin, request))
            sys.stdout.buffer.write(struct.pack('!I', len(response)))
            sys.stdout.buffer.write(response)
            sys.stdout.buffer.flush()
    except Exception:
        sys.stderr.write('STRATEGY_WORKER_EVALUATION_FAILED\n')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
