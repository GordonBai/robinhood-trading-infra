# External plugin quickstart

This tutorial uses only an installed `martin-agentic` wheel, the public
example distribution source, and synthetic files. It grants no trading,
approval, provider or deployment authority.

## 1. Prerequisites and release layout

You need:

- Python 3.11 or newer;
- a C-free wheel build path (`python -m build` with its build backend
  available, or `uv build` with the build dependencies already cached);
- a release directory containing `manifest.json` and
  `source-and-tests.tar.gz` from `martin-agentic release-bundle build`.

The release archive is **source plus test inputs**. It does **not** contain a
`dist/` wheel. In particular, do not look for a wheel after extracting it, and
do not treat a private path from someone else's extraction as an install path.

Work from absolute paths. The commands below use `/abs/release` for the
verified release directory, `/abs/src` for a new extraction target and
`/abs/venv` for a new virtual environment. `release-bundle verify` refuses to
overwrite an existing extraction directory.

Because the archive is source, not a wheel, you can bootstrap without an
existing installation. First compare the archive hash with `archive_sha256` in
`manifest.json` using a trusted copy of that value:

```sh
shasum -a 256 /abs/release/source-and-tests.tar.gz
mkdir /abs/src
tar -xzf /abs/release/source-and-tests.tar.gz -C /abs/src
python -m venv /abs/venv
. /abs/venv/bin/activate
python -m ensurepip --upgrade
python -m pip install build
```

If a previously verified `martin-agentic` CLI is available, prefer its
`release-bundle verify` path; it also checks the source inventory and file
hashes:

```sh
martin-agentic release-bundle verify \
  --directory /abs/release \
  --expected-source <trusted-source-fingerprint> \
  --expected-input-set <trusted-input-set-sha256> \
  --extract-to /abs/src-verified
```

The expected hashes must come from the trusted test record for that release;
copying them only from an untrusted archive is not provenance. The manual
`tar` path verifies the archive bytes but not each source file, so use the CLI
verification when available. With no network, use
`uv build --wheel --offline` against cached build dependencies instead of
`python -m build`; the tutorial never needs a live market provider.

Build and install the runtime wheel from the extracted source:

```sh
cd /abs/src
python -m build --wheel --outdir dist
# offline equivalent: uv build --wheel --out-dir dist --offline
python -m pip install dist/martin_agentic-0.1.0-py3-none-any.whl
```

The extracted source tree is a qualified input inventory. Do **not** run a
plugin build or write a report output inside
`/abs/src/examples/plugins/example_momentum`: setuptools creates
`build/lib/.../*.py`, and that would become part of the qualified source set
and invalidate the installed-source identity. Copy the template to a new work
directory first, exactly as the public build workflow does:

```sh
mkdir -p /abs/work /abs/work/output
cp -R /abs/src/examples/plugins/example_momentum /abs/work/example_momentum
cd /abs/work/example_momentum
python -m build --wheel --outdir dist
# offline equivalent: uv build --wheel --out-dir dist --offline
python -m pip install --no-deps --force-reinstall \
  dist/trading_example_momentum-1.0.0-py3-none-any.whl
```

The copied directory is outside the extracted candidate. The main wheel's
`/abs/src/dist` directory is also outside the qualified set; it is the exact
wheel installed above and is not a search path or a source-tree fallback.
The second wheel is a separate distribution, not a module inside
`martin-agentic`. Its entry points are `example-momentum` and
`example-mean-reversion`.

## 2. Inspect installed providers

```sh
martin-agentic plugin list
```

Expected: the two entry points above, each with its distribution name,
version and actual installed-file digest. No infrastructure source edit adds a
provider.

## 3. Run conformance

Run from the copied work directory so the relative JSON paths are explicit:

```sh
cd /abs/work/example_momentum
martin-agentic plugin check --plugin example-momentum --config config.json
```

Exit `0` means descriptor, create, decision binding, checkpoint, exit rule and
instance-isolation checks passed. The exit-rule check uses a fixed synthetic
feature/quote view to validate output shape; it is not evidence of a real
checkpoint-to-exit integration. Exit `2` is a contract/config failure; exit
`3` means the entry point is not installed. The command emits structured JSON
and never contacts a broker. Python plugins are trusted code; the bounded probe
is fault isolation, not a sandbox.

## 4. Offline advisory shadow

`plugin shadow` uses the real instance host and bounded external worker but
creates **no Book action**. It is an advisory replay, not a complete execution
lifecycle.

```sh
martin-agentic plugin shadow \
  --plugin example-momentum \
  --dataset /abs/work/example_momentum/events-momentum.json \
  --config /abs/work/example_momentum/config.json
```

Expected: `advisory_only=true`, `book_actions=0`, a plugin checkpoint and any
proposal rows. No provider or broker is contacted.

## 5. Reproducible synthetic backtests

The two rules have separate datasets so both produce a full entry/exit round
trip under the default 20-second proposal/advice TTL and the entry ceiling.
Events are ten seconds apart.

```sh
cd /abs/work/example_momentum
mkdir -p /abs/work/output
martin-agentic plugin backtest \
  --plugin example-momentum \
  --dataset events-momentum.json --config config.json \
  --output /abs/work/output/momentum.json
martin-agentic plugin backtest \
  --plugin example-mean-reversion \
  --dataset events-mean-reversion.json --config config.json \
  --output /abs/work/output/mean.json
```

Expected with the default `fee_per_share=0.01`,
`slippage_per_share=0.01`, `initial_equity=10000`:

| Rule | Trades | Reason | Final equity | Gross / fees / slippage / net |
|---|---|---|---|---|
| momentum | 1 | `MOMENTUM_LOST_AVERAGE` | `10000.65` | `0.69 / 0.02 / 0.02 / 0.65` |
| mean-reversion | 1 | `MEAN_REVERSION_REACHED_AVERAGE` | `10000.75` | `0.79 / 0.02 / 0.02 / 0.75` |

The entry decision at event *i* is eligible at *i+1*; the strategy exit advice
also executes at the next event. `gross_pnl` is raw exit bid minus raw entry
ask; fees and slippage are subtracted separately to obtain `net_pnl` and
`final_equity`. The report includes an independent reconciliation flag,
`open_position`, `end_of_data_policy` and `production_ready=false`.

The final event is never treated as if a next event existed. The default
`LIQUIDATE_AT_LAST_BID` policy closes any remaining position with the distinct
reason `END_OF_DATA_LIQUIDATION`; pass `end_of_data_policy='NONE'` through the
API to leave it open and reported instead. This liquidation is not a strategy
or structural-stop execution.

Negative control: the next dataset keeps the momentum signal but places the
fill candidate 21 seconds after the entry decision, beyond the default TTL.

```sh
martin-agentic plugin backtest \
  --plugin example-momentum \
  --dataset events-expired.json --config config.json \
  --output /abs/work/output/expired.json
```

Expected: `trade_count=0` and a `LIMIT_EXPIRED` row in `unfilled_orders`.
Do not lengthen the production default TTL to make a tutorial green.

## 6. Deployment configuration (not run offline)

`policy.json` (copied to `/abs/work/example_momentum/policy.json`) contains
an explicit `strategy_instances` entry for `example-momentum`. Use that exact
path with the separate deployment command; never point it at the extracted
source tree. Deployment is a separate operation: it requires a saved
read-only source and its documented prerequisites. It is not part of this
offline tutorial and must not be run against a real provider by an Agent
following this page. See `BACKTEST_AND_DEPLOY.md` for the supported deployment
form and its non-claims.

The same `ExecutionBook`, account admission, symbol lease, attribution,
protection/management and recovery owners are used. The plugin cannot place or
cancel an order and receives no account/broker handle.

## 7. Multi-instance isolation

Use two specs with different configs and/or different plugin ids. Each gets its
own checkpoint and worker. Account budget, symbol leases and execution
ownership remain shared infrastructure decisions. A second instance watching
the same symbol cannot borrow the first instance's features or checkpoint.

## Troubleshooting

| Error | Fix |
|---|---|
| `PLUGIN_NOT_INSTALLED` | install the wheel in the active environment |
| `PLUGIN_ENTRY_POINT_AMBIGUOUS` | remove duplicate entry points |
| `PLUGIN_PROTOCOL_VERSION_UNSUPPORTED` | implement protocol version `1` |
| `PLUGIN_CONFIG_UNKNOWN_FIELD` | remove fields not declared in `config_schema` |
| `PLUGIN_ID_MISMATCH` | entry-point name must equal `plugin_id` |
| `PLUGIN_IDENTITY_MISMATCH` | reinstall the exact distribution; do not edit installed files |
| `PLUGIN_BACKTEST_QUOTE_TIME_FUTURE` | quote time is later than its event; do not reuse a future quote |
| `PLUGIN_BACKTEST_QUOTE_TIME_STALE` | quote age exceeds `max_quote_age_seconds` |
