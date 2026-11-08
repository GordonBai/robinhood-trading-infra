# Bounded read-only collection

`martin-agentic bounded-read` is the public, current-source entry point for a
bounded read-only collection session. It reuses `run_live_session`, the provider
read/collector path, owned read transports and the process registry. It does not
place, review, cancel or approve an order and does not create a second collector.

## Preflight (default, no side effects)

Preflight performs no auth, getter, target or provider action. It requires an
explicit runtime policy, expected source fingerprint, expected protected-file
manifest and process-registry path:

```sh
martin-agentic bounded-read \
  --policy /abs/runtime/read-policy.json \
  --target /abs/runtime/read-089 \
  --expected-source <current-source-fingerprint> \
  --expected-protected /abs/runtime/protected.json \
  --registry /abs/runtime/read-registry.jsonl
```

`--expected-protected` is a JSON object mapping each protected file path to its
SHA-256. The files and hashes are supplied by the owner/runtime, not by the
command itself. The policy must declare `capture_only=true`, `live_enabled=false`
and `scheduled=false`. Preflight fails if the source/pin cannot be bounded, a
protected file is missing/mismatched, the target exists, the registry is not
readable, or a matching bounded-read process is already alive.

The public `examples/readonly/policy.json` is a synthetic `0000` policy for
shape checks only; it is not a real account or authorization. Each execute run
claims a fresh target directory; an existing target is refused and must not be
reused for a later run.

## Execute (only after explicit approval)

Add `--execute` and `--symbols` only in a separately approved window. The child
uses the existing real read-only session path (`run_live_session` with
`capture_only`, disabled agent mode). This task does not run it.

```sh
martin-agentic bounded-read ... --symbols DEMO --execute
```

Budgets are fixed at the entry point: at most 3 cycles, 120 getter reservations,
90 seconds total wall clock including cleanup, and a 2-second cleanup reserve.
Startup/connect/auth/getter/owner wait/save/cleanup share that budget.

Execute is atomic per target. After a side-effect-free preflight, the parent
claims the target with one exclusive `mkdir` (two racing executors: one wins,
the other fails closed), writes a sealed `inputs.json` plus `claim.json` with the
run id, verified source fingerprint, source-pin digest, policy bytes digest and
protected hashes, then spawns one registered child. The child rechecks the same
generation and protected files before creating a transport and again immediately
before the first getter; any drift fails closed without dispatch. stdout and
`summary.json` are accepted only under this run id; summary files from another
run are stale evidence and never counted.

The parent re-derives PASS from the child's append-only `cycles.jsonl` journal
and child facts, not from the child's status label. PASS requires all configured
cycles completed, a run-identity match, at least one real getter within budget,
successful account/market observations, bounded capture/session completion, and
verified registry cleanup. A timeout kill retains completed cycles and actual
getter reservations in the journal, and the result reports them as partial.
Registry entries owned by this run are killed and re-verified — a surviving
independent session is `resource_state=UNRESOLVED`, never `CLEAN`.
`status=FAILED`, `resource_state=UNRESOLVED`, or fewer completed cycles than
configured is never a successful completion. The output JSON includes run id,
source/pin, protected hashes, cycles configured/completed, getter calls,
wall/cleanup deadlines and `entry_authorized=false`.

The runtime-only `runtime/iteration-068/readonly_multicycle.py` remains private
historical evidence and is not the public entry point. Its default source pin is
old; do not use it as current-source qualification.
