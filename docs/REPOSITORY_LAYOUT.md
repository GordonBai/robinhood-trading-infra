# Repository layout and publication boundary

The final GitHub repository root is this repository. The current public delivery is the source tree and its release manifest, not a Git commit.

## Public source

- `infra/src/trading_contracts/` — public contracts.
- `infra/src/trading_infra/` — generic instance, budget, lease, attribution, accounting, calendar and host queue.
- `infra/src/martin_agentic/` — generic execution, broker facts, risk, protection/exit, Host/worker, audit/recovery and CLI core.
- `strategy/martin_luk/src/martin_luk/` — Martin strategy/application composition.
- `strategy/martin_luk/src/trading_strategies/` — Martin plugin and compatibility imports.
- `strategy/examples/src/trading_example_plugins/` — independent vwap/slow synthetic plugin families.
- `strategy/examples/src/trading_example_app/` — example catalog, exit rules and generic session composition.
- `examples/plugins/example_momentum/` — independent buildable external-plugin template and two synthetic rules.
- `tests/`, `config/project.json`, `config/current_capture.example.json`, `config/synthetic_policy.json`, synthetic `examples/`, `strategy-spec/`.
- `README.md`, `pyproject.toml`, `uv.lock`, public `docs/` (except `docs/superpowers/`).

## Local-only material

- `plan/`, private `reports/`, `strategy/martin_luk/research/`, `strategy/martin_luk/config/live_trial_*`, `strategy/martin_luk/config/read_only_capture_*`. The single public metadata exception is `reports/community_tool_contract_evidence.json`.
- `runtime/`, ledgers, SQLite sidecars, logs, PIDs.
- `skills/`, `sites/`, notebooks, raw recordings and screenshots; `scripts/*` except the four public qualification exceptions.
- credentials, `.env*`, private keys and local editor artifacts.

Ignoring a path does not untrack a file already in the Git index, and removing an index entry does not remove it from existing Git history. Review the local publication checklist before publication; this task did not stage, untrack, delete, commit, rewrite history or push.

## Checks

```bash
git check-ignore --no-index -v runtime/... plan/... strategy/martin_luk/research/...
git ls-files -ci --exclude-standard
git status --porcelain=v1 -uall
git diff --cached --stat
```

## Current non-claims

The executable, test, config and lock tree is byte-identical to the accepted
local baseline `ee4f4871…`; its installed full qualification is 2737 tests, 2723
passed, 14 optional audit-SDK skips, 0 failures/errors. This revision changes
public documentation only. That is local package/full evidence only, not a
deployment or formal/provider acceptance. Real provider access, formal Host delivery,
unattended/per-order-confirmed support, fresh real quote acceptance, the normal
real protection close loop, OS/machine recovery, continuous formal read-only operation and normal real fill protection/exit remain unverified.
This tree is not deployed; live execution is disabled; `production_ready=false`.
