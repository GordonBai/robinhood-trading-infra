# Backtest, shadow and deployment

One `decide` interface is used by conformance, backtest and the instance
runtime. This document describes only implemented commands.

## Synthetic backtest

`plugin backtest` replays explicit synthetic quote events through the installed
plugin. The dataset declares `schema_version`, `synthetic=true`,
`max_quote_age_seconds` (default 15) and one or more quote events. Every event
`at` and every present `data.bid_at`, `data.ask_at` and `data.observed_at` must
be aware UTC. Missing synthetic quote times are explicitly derived as the
event time and counted in `time_model.derived_quote_fields`; existing values
are never overwritten. Future, stale, naive, duplicate-id and out-of-order
inputs are rejected. A future 2099 quote cannot be used in a 2026 event.

A decision at event *i* creates an order eligible only from event *i+1*. Entry
fills at ask plus configured slippage only when that price is at or below the
proposal's entry ceiling; otherwise the limit remains unfilled until expiry.
Exit fills at bid minus slippage. Orders expire at proposal/exit-rule validity;
the rule advice receives the documented 20-second advisory TTL and is eligible
on the next event. A structural stop has no strategy TTL, wins over
HOLD/strategy exits, and executes on the next eligible event. Strategy
`EXIT`, `REDUCE` and `TIGHTEN_PROTECTION` fields are accepted when supplied and
malformed quantities/stops/validity are rejected explicitly.

The dataset must be single-symbol; mixed symbols are rejected rather than
mixing prices. A plugin whose descriptor requires anything other than
`quotes`, or declares warm-up/history bars, is rejected by this quote-only
backtest instead of silently pretending the data exists.

Cash is capped by `initial_equity`; no financing is assumed. For a closed
trade, `gross_pnl` is raw exit bid minus raw entry ask, `fees` is charged on
both sides, `slippage` is charged on both sides, and
`net_pnl = gross_pnl - fees - slippage`. `final_equity - initial_equity` is
checked independently. The model has no queue position, market impact, partial
fills, borrow or short model, and its P&L is not evidence of profitability.

The final event has no next event. An exit created there is reported as
`EXIT_NOT_EXECUTED_AT_END`; it is not executed as though a next event existed.
The default `END_OF_DATA` policy may close a remaining position at the last
bid, but labels that trade `END_OF_DATA_LIQUIDATION`, separately from strategy
or structural-stop execution. `end_of_data_liquidation` is true only when such
a trade exists; `end_of_data_liquidation_policy` reports the configured policy.
The API value `end_of_data_policy='NONE'` leaves the position open and reported
in `open_position`.


`plugin check` uses a fixed synthetic feature/quote view for the exit rule; it validates the
output contract, not integration with a live checkpoint. A plugin that ignores
`allocation.execution` remains compatible; a plugin using it gets only its own confirmed
quantity/lifecycle facts and should not treat pending/approval/UNKNOWN/stale facts as flat.

## Offline advisory shadow

`plugin shadow` runs the real instance host and bounded external worker over
synthetic events and reports proposals/checkpoint with `book_actions=0`. It is
advisory-only; it does not create Book orders or complete an exit.

## Shadow/runtime

The deployment path uses the same `ExecutionBook`, instance admission, shared
account budget, symbol leases, attribution, protection and recovery owners as
built-in plugins. It requires an explicit saved/read-only source; the offline
tutorial does not run this command. Use `live-session --strategy external --strategy-plugin
<id> --policy <policy>` with an explicit `strategy_instances` list. The
instance's mode controls provenance (`shadow` synthetic, `read_only_shadow`
real read-only); neither mode grants order authority. Do not claim a real
read-only run without a saved read-only source and its preconditions.

## Deployment and operations

`session-control start ... --strategy external --strategy-plugin <id> --strategy-plugin <id>` forwards each explicit plugin as its own flag; the child argv uses repeated `--strategy-plugin` options. The controller records and verifies the composition in its process identity. Stop/status use the existing control
records. The process is disabled by default; no real daemon or provider is
started by this tutorial. Formal Host delivery, per-order confirmation, continuous formal
read-only operation, normal real fill protection/exit and unattended approval remain separate, unverified
capabilities.

## Evidence to keep

Record the source fingerprint, plugin distribution digest, config, dataset
hash, command, exit code, report, status file and audit/DB paths. A green
contract check or backtest is not a production authorization.
