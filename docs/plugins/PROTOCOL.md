# Strategy plugin protocol v1

Status: implemented in this repository. External Python plugins are **trusted
code**; the contract boundary is not an OS sandbox and grants no order access.

## 1. Entry point

An installed distribution registers a provider in the entry-point group
`trading_infra.strategies`:

```toml
[project.entry-points."trading_infra.strategies"]
my-strategy = "my_package:MyProvider"
```

The provider MUST expose:

```python
def describe() -> trading_infra.plugin_protocol.PluginDescriptor: ...
def create(config: dict) -> object: ...
```

`create` returns an object with `decide(request: StrategyInput) -> StrategyDecision`.
The entry-point name MUST equal `descriptor.plugin_id`. Duplicate entry points,
unknown ids, unloadable providers, wrong descriptor types and protocol versions
MUST fail closed. Infrastructure source MUST NOT be edited to add a plugin.

## 2. Descriptor and manifest

`PluginDescriptor` fields:

| Field | Requirement |
|---|---|
| `plugin_id` | lowercase id; unique entry-point name |
| `version` | distribution-owned semantic string |
| `protocol_version` | MUST be `1` |
| `state_schema` | plugin-owned checkpoint schema name |
| `config_schema` | closed flat scalar JSON-schema subset |
| `data_requirements` | non-empty subset of `quotes`, `5minute-bars`, `universe-screen` |
| `capabilities` | advisory labels only; never execution authority |
| `feature_keys` | checkpoint keys this plugin publishes for exit advice |
| `exit_rule` | optional `module:function`; requires `feature_keys` |
| `warmup_bars` / `history_bars` | non-negative declared history bounds; 0 means no declared warm-up. The quote-only backtest refuses non-zero values instead of synthesizing history. |

`PluginManifest` is built by the host from the descriptor and validated config.
The manifest binds plugin id/version, state schema, config, data requirements,
capabilities and max in-flight. Config values MUST be closed flat scalars
(string, integer, boolean); decimal quantities SHOULD be strings to preserve
precision. Unknown config fields MUST be rejected. The infrastructure adds its
own `cost_buffer_per_share` to the registered manifest; it is not a plugin
authoring field.

## 3. Lifecycle

1. **prepare**: the host pins an event and the exact checkpoint version.
2. **decide**: the plugin receives immutable `StrategyInput`; it MUST return a
   `StrategyDecision` bound to that request. Proposals MUST carry request
   identity, source hash, validity and a stable proposal id.
3. **commit**: the host performs checkpoint compare-and-set and event
   consumption. Duplicate event identity is idempotent; changed content is a
   conflict. Expired decisions MUST be rejected.
4. **feedback/recovery**: the host owns retries, UNKNOWN, claim/lease,
   protection, receipts, attribution and account reconciliation. A plugin MUST
   NOT be given a broker handle, account number, credential or approval id.
5. **stop**: workers are bounded and reap their child; plugins MUST terminate
   on EOF or timeout.

Checkpoints MUST be JSON-compatible scalar/mapping data, bounded and
deterministic. A state schema change MUST use a new schema/version and MUST NOT
silently reinterpret old state.

## 4. Identity and reproducibility

The host hashes the installed distribution's actual files and records:
distribution name/version, file digest, descriptor, config fingerprint,
instance id, mode and source fingerprint. A changed package or config MUST
invalidate an outstanding worker. Workers run with `python -I` from the
installed package, never from an editable path, workspace `src`, or the old 085
tree. A manifest version string alone is not identity proof.

## 5. Inputs

- Events are point-in-time quote/bar/universe events with explicit event time,
  availability/observation time and source hash.
- Prices are decimal; symbols follow uppercase equity/ETF syntax.
- Stale, future, out-of-order or provenance-free events MUST be rejected by the
  host. Bar use MUST respect exchange availability and regular-session bounds.
- Warm-up/history requirements MUST be declared in the descriptor. The current
  quote-only backtest rejects non-zero `warmup_bars`/`history_bars`; missing
  history is a data gap, never silently synthesized.

## 6. Outputs

`StrategyInput.allocation` is an advisory allocation/context mapping. In addition to
`max_cash`, `max_planned_loss`, `max_notional` and `max_concurrent`, the infrastructure adds an
`execution` mapping. It contains only this instance's execution facts:
`instance_id`, `lifecycle` (`not_executed`, `pending`, `partial`, `protected`, `exit_pending`,
`flat_verified`, `unknown`), `confirmed_quantity`, `available_quantity`,
`available_quantity_state`, `feedback_version`, `observed_at`, `fact_fresh`,
`observation_at`, `observation_fresh`, `owner_state`, `owner_action_at`,
`owner_action_kind`, `owner_trade_id`, `owner_activity_source`, `source` and
`accounting_state`. It never contains an account, broker object, order/approval/ref identity
or another instance's data.

Two clocks are separate. `observed_at`/`fact_fresh` are the latest activity fact (fill,
protection, exit, flat, rejection). `observation_at`/`observation_fresh` are the execution
owner's latest complete broker observation. A long-idle position that is freshly observed flat
is `flat_verified`; an old flat event with no fresh observation is not. `owner_state` exposes the
current management-action state (`READY`, `RUNNING`, `WAITING_USER`, `UNKNOWN`) for trades this
instance owns; an older flat event never overrides a newer pending/UNKNOWN fact.
`available_quantity` is the string `unknown` unless a fresh confirmed sell reservation/position
proves it; it is never assumed equal to `confirmed_quantity`. Plugins must treat `unknown` as
unknown, not as a number. Plugins that ignore the field remain compatible. It is advice context,
not execution authority; infrastructure still owns budget, lease, stop, risk, protection and
fills.

`StrategyDecision` contains proposals, next checkpoint and diagnostics.
`StrategyProposal` actions are `ENTER`, `REDUCE`, `EXIT`,
`TIGHTEN_PROTECTION`, `HOLD` in the contract. The synthetic backtest honors
all four non-HOLD actions with next-event eligibility and rejects malformed
quantities, stops or validity windows. The current live instance admission path
accepts only `ENTER`; `REDUCE`, `EXIT` and `TIGHTEN_PROTECTION` proposals are
refused as `ADMISSION_ACTION_UNSUPPORTED` unless a future infrastructure adapter
is explicitly implemented. Managed live exits use `exit_rule` and the existing
Book/exit-advisor path, not an admission action. There is no
take-profit order API, no broker order id and no approval bypass. Proposal
expiry and idempotency are enforced by the host. A plugin recommendation is
NEVER an execution authorization.

## 7. Exit advice

A descriptor MAY declare `exit_rule = "module:function"` and one or more
`feature_keys`. The function receives a read-only view with position, quote,
features and as-of time and MUST return one of `EXIT`, `REDUCE`,
`TIGHTEN_PROTECTION`, `HOLD` with a reason. Infrastructure structural stops and
risk rules always win. A plugin cannot loosen a stop. The function runs in a
bounded advisory process; timeout/unavailable output is a HOLD/refusal, never a
fallback to another strategy.

## 8. Isolation

Every instance has its own checkpoint, queue, state schema, config and
attribution. Instances share only the account budget, symbol-lease authority
and execution/risk book. Two instances may watch the same symbol; symbol/lease
and ownership conflicts are resolved by infrastructure, and one instance's
features MUST NOT be borrowed by another. A plugin has no direct order,
cancel, approval or account API.

## 9. Modes

| Mode | Data | Execution capability |
|---|---|---|
| `shadow` | synthetic | none |
| `read_only_shadow` | real read-only | none |
| `disabled` | none | none |

There is no live execution capability in the external plugin path. Read-only
mode requires an explicit saved/read-only source and still cannot place orders.
Research results and deployment modes do not grant per-order confirmation or
unattended authority.

## 10. Conformance and error codes

```sh
martin-agentic plugin list
martin-agentic plugin check --plugin my-strategy --config config.json
```

The check uses the real contracts/host and emits structured JSON. Exit codes:
`0` pass, `2` contract/config/conformance failure, `3` plugin not installed.
Stable error examples: `PLUGIN_NOT_INSTALLED`, `PLUGIN_ENTRY_POINT_AMBIGUOUS`,
`PLUGIN_PROTOCOL_VERSION_UNSUPPORTED`, `PLUGIN_DESCRIPTOR_CONTRACT_REQUIRED`,
`PLUGIN_CONFIG_UNKNOWN_FIELD`, `PLUGIN_IDENTITY_MISMATCH`,
`PLUGIN_EXIT_RULE_LOAD_FAILED`. A malicious or timed-out worker is bounded and
rejected; this is not a claim of safe execution for untrusted Python.

## 11. Machine schemas and template

- [Descriptor schema](schema/descriptor.schema.json)
- [Manifest schema](schema/manifest.schema.json)
- Buildable template: `examples/plugins/example_momentum/` (`pyproject.toml`,
  two providers, synthetic events/config and expected CLI workflow).
