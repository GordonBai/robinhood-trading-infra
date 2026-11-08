# Agent authoring guide

An Agent can complete this workflow without reading `plan/`, progress files or
chat history, and without real credentials.

## Allowed changes

An external plugin Agent MAY create or edit only its own distribution:
`pyproject.toml`, its Python package, its descriptor/factory, its tests and its
synthetic data. It MUST NOT edit `infra/src`, built-in catalogs, CLI branches,
risk, execution, protection, audit or fixture files.

## Interface index

- `trading_infra.plugin_protocol.PluginDescriptor` — versioned descriptor.
- `trading_infra.plugin_protocol.load_plugin` / `list_plugins` — installed
  entry-point discovery and actual distribution identity.
- `trading_contracts.strategy.StrategyInput` / `StrategyDecision` /
  `StrategyProposal` — immutable advisory contracts.
- `trading_infra.strategy_host.evaluate_strategy` — host validation.
- `trading_infra.external_application.ExternalCatalog` — config, worker,
  feature and exit-rule integration.
- `trading_infra.plugin_conformance.run_conformance` — real contract checks.
- `trading_infra.plugin_backtest.run_backtest` — deterministic synthetic fills.
- `request.allocation.execution` — instance-owned confirmed quantity/lifecycle feedback; read-only, not an order/account API. Backtest uses `backtest_model`; advisory shadow uses `not_executed`.

## Completion criteria

1. `plugin list` shows the provider and a digest.
2. `plugin check` exits 0 with every check `passed=true`.
3. `plugin backtest` exits 0, produces deterministic trades and limitations, uses aware UTC quote times, and rejects unsupported data requirements/warm-up rather than synthesizing them.
4. A two-instance session keeps separate checkpoints and features.
5. No source file outside the plugin distribution was modified.

## Trust boundary

The plugin boundary is a dependency and validation boundary, not a sandbox.
Do not load untrusted code or grant it broker credentials. The plugin returns
advice only; account risk, budgets, leases, order lifecycle, protection,
recovery and approval remain infrastructure concerns. Backtest P&L is
research arithmetic, not execution authority or a profitability claim.
