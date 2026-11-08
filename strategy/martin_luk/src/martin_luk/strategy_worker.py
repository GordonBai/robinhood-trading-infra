"""Trusted application composition for one isolated advisory computation.

Receives contract bytes only. No execution book, broker client or model is
constructed. This subprocess is fault isolation, not an untrusted-code sandbox.
"""
import argparse
import sys

from trading_contracts.codec import decode_input, encode_decision
from trading_contracts.instance import config_version
from trading_infra.market_calendar import ExchangeCalendarView
from trading_infra.strategy_host import evaluate_strategy
from trading_strategies.martin.signals import MartinSignalPlugin


MAX_INPUT_BYTES=16*1024*1024


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--cost-buffer-per-share',default='0.05')
    args=parser.parse_args()
    try:
        raw=sys.stdin.buffer.read(MAX_INPUT_BYTES+1)
        if len(raw)>MAX_INPUT_BYTES:raise ValueError('INPUT_TOO_LARGE')
        request=decode_input(raw)
        plugin=MartinSignalPlugin(calendar_view=ExchangeCalendarView(),
            cost_buffer_per_share=args.cost_buffer_per_share)
        expected_config=config_version(plugin.PLUGIN_ID,plugin.VERSION,
            {'cost_buffer_per_share':str(plugin.cost)})
        if request.config_version!=expected_config:
            raise ValueError('STRATEGY_WORKER_CONFIG_MISMATCH')
        decision=evaluate_strategy(plugin,request)
        sys.stdout.buffer.write(encode_decision(decision))
        sys.stdout.buffer.flush()
        return 0
    except Exception:
        # Diagnostics must not leak raw inputs, private paths or plugin errors.
        sys.stderr.write('STRATEGY_WORKER_EVALUATION_FAILED\n')
        return 1


if __name__=='__main__':
    raise SystemExit(main())
