# Example external plugins

This independent distribution registers two entry points in
`trading_infra.strategies`: `example-momentum` and `example-mean-reversion`.
They are synthetic conformance examples, not trading advice.

```sh
# Copy this directory outside the extracted source candidate first.
# Building in place creates build/lib/*.py, which is qualified source.
python -m build --wheel --outdir dist
python -m pip install --no-deps --force-reinstall \
  dist/trading_example_momentum-1.0.0-py3-none-any.whl

# After copying, from /abs/work/example_momentum:
martin-agentic plugin list
martin-agentic plugin check --plugin example-momentum --config config.json
martin-agentic plugin backtest --plugin example-momentum \
  --dataset events-momentum.json --config config.json
martin-agentic plugin backtest --plugin example-mean-reversion \
  --dataset events-mean-reversion.json --config config.json
martin-agentic plugin backtest --plugin example-momentum \
  --dataset events-expired.json --config config.json
martin-agentic plugin shadow --plugin example-momentum \
  --dataset events-momentum.json --config config.json
```

The example reads `request.allocation.execution` when present. A confirmed
flat (or `not_executed`) resets the advisory `in_position` flag; a pending,
partial, protected or UNKNOWN state does not. The checkpoint remains plugin-owned,
while infra owns the actual position and protection facts.

The two round-trip datasets are ten seconds apart and use the default
20-second TTL. Expected backtest results with default fees/slippage:
momentum has one `MOMENTUM_LOST_AVERAGE` trade and final equity `10000.65`;
mean-reversion has one `MEAN_REVERSION_REACHED_AVERAGE` trade and final equity
`10000.75`. `events-expired.json` is the negative control: zero trades and a
`LIMIT_EXPIRED` unfilled order.

`plugin shadow` is advisory only (`book_actions=0`). `policy.json` is for an
explicit future deployment with a saved read-only source; it is not a command
for this offline tutorial.
