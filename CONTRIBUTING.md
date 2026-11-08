# Contributing

Thanks for helping improve Robinhood Trading Infra. Issues, bug reports,
documentation fixes, plugin contributions and pull requests are welcome.

## Before you start

- Search existing issues and pull requests before opening a duplicate.
- For a bug, include the smallest reproducible command or synthetic dataset,
  the expected and actual result, the Python/OS version, and the relevant
  non-secret log lines.
- For a larger design change, open an issue first so the scope and safety
  boundary can be agreed before implementation.
- Do not include credentials, API keys, account numbers, broker data, ledgers,
  raw private recordings, personal research material or unreviewed third-party
  content in an issue or pull request.

## Development setup

```bash
uv sync --frozen
uv run --frozen martin-agentic --help
uv run --frozen martin-agentic dry-run --scenario examples/dry_run_500.json
```

The offline test suite uses synthetic policies and temporary databases. Read
the [plugin quick start](docs/plugins/QUICKSTART.md) before changing plugin
behavior or the [architecture](docs/ARCHITECTURE.md) before changing the
infrastructure layers.

## Dependency direction

- `trading_contracts` must not import concrete infrastructure or strategy code.
- `trading_infra` is generic: no module-level strategy import. An application
  composition may be selected lazily by the CLI.
- `martin_agentic` owns execution lifecycle, risk, protection/exit and recovery;
  strategy algorithms belong in the strategy/plugin layer.
- External providers use the versioned `trading_infra.strategies` entry-point
  protocol and never receive a broker handle or order authority.

Keep the public package and CLI names stable unless the change is explicitly a
breaking release. Do not add a new abstraction when an existing contract or port
already owns the behavior.

## Tests

- Add or update tests for the behavior you change.
- Run the relevant targeted tests, not only the whole suite.
- A full suite is appropriate for a large milestone or before a release; small
  documentation or targeted changes do not require a new full run.
- Tests must use synthetic data and temporary ledgers/workspaces. Never use real
  account state or credentials in a test.
- Backtests are evidence about the model and code path, not evidence of profit
  or live readiness.

## Execution and risk changes

Any pull request that touches order placement, cancellation, approval,
protection/exit, risk limits, account admission, reconciliation or recovery
must include:

- the exact offline scenario and command used;
- observed return codes and relevant evidence fields;
- a statement of what remains unverified against a real broker;
- explicit confirmation that formal per-order confirmation is not bypassed by a
  local file, boolean or agent prompt.

Do not enable live execution, add a default live permission, or weaken the
30-second protection, risk, leverage, overnight or confirmation boundaries in a
contribution.

## Plugins and third-party content

- A new plugin must declare its manifest, data requirements and capabilities and
  pass `plugin check`/conformance and a synthetic backtest.
- Do not vendor third-party strategy research, code or data unless its license
  and attribution are clearly compatible and included. Do not claim an
  affiliation with, or endorsement by, a broker or third-party author.
- Plugins are trusted Python code, not a sandbox. Review plugin code accordingly.

## Pull requests

- Keep the change focused and explain the why.
- Update the relevant public documentation when behavior changes.
- Include the tests/commands you ran and their results.
- No CLA or DCO sign-off is required.
- By submitting a contribution, you agree to license your contribution under
  the project’s [MIT License](LICENSE).

Use the pull-request template for the short checklist. Maintainers may close a
change that crosses the safety boundary or cannot be verified offline.
