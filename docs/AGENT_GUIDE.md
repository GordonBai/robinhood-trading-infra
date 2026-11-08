# Agent guide

## Scope

This repository is an offline/review candidate. Live trading is disabled. Agents must not use this tree to authenticate a real provider, review/place/cancel orders, approve a trade, start a live daemon, modify real ledgers/risk files or publish to GitHub.

## Commands

```bash
uv sync --frozen
uv run --frozen martin-agentic --help
uv run --frozen python -m unittest discover -s tests -v
```

Offline/synthetic examples:

```bash
uv run --frozen martin-agentic dry-run --scenario examples/dry_run_500.json
uv run --frozen martin-agentic fault-rehearsal --output-dir /tmp/trading-rehearsal
```

Do not invoke `live-session`, `session-control`, native review/place/cancel, provider reads or any command requiring credentials for a release check.

## Paths

- Generic infra: `infra/src/trading_contracts`, `infra/src/trading_infra`, `infra/src/martin_agentic`.
- Martin strategy/application: `strategy/martin_luk/src/martin_luk`, `strategy/martin_luk/src/trading_strategies`.
- Independent example application: `strategy/examples/src/trading_example_plugins`, `strategy/examples/src/trading_example_app`.
- External protocol and template: `infra/src/trading_infra/plugin_protocol.py`, `examples/plugins/example_momentum/`, `docs/plugins/`.
- Synthetic policies: `config/synthetic_policy.json`, `tests/fixtures/policies/`.
- Private date/account policies and research are local-only and ignored.

Never import `martin_luk` or `trading_strategies` from generic infra at module import. CLI strategy commands may select a strategy implementation lazily; the generic CLI/help path must not load Martin algorithms.

## Failure rules

- `PLUGIN`/strategy failures must be reported as advisory failure, not converted to fills or protection.
- UNKNOWN means reconcile from broker facts; never blindly retry writes.
- A local file, process PID, boolean or agent message never proves broker confirmation or protection.
- State/receipts/audit evidence is local integrity evidence, not immutable custody.

Report actual command return codes and outputs. Do not claim `production_ready`, GitHub publication, formal Host, continuous formal read-only operation or normal real fill protection/exit from an offline test.
