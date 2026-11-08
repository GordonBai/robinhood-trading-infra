"""Warm example-plugin advisory worker. No broker and no execution book."""
import argparse
import struct
import sys

from trading_contracts.codec import MAX_BYTES, decode_input, encode_decision
from trading_contracts.instance import config_version
from trading_infra.market_calendar import ExchangeCalendarView
from trading_infra.strategy_host import evaluate_strategy

from . import catalog


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
    parser.add_argument('--cost-buffer-per-share', default='0.05')
    parser.add_argument('--source-fingerprint')
    args = parser.parse_args()
    try:
        plugin = catalog.create(args.plugin, calendar_view=ExchangeCalendarView(),
                                cost_buffer_per_share=args.cost_buffer_per_share)
        expected_config = config_version(
            plugin.PLUGIN_ID, plugin.VERSION,
            {'cost_buffer_per_share': str(plugin.cost)})
        if args.source_fingerprint is not None:
            from martin_agentic.qualification import source_fingerprint
            if source_fingerprint() != args.source_fingerprint:
                raise ValueError('STRATEGY_WORKER_SOURCE_MISMATCH')
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
