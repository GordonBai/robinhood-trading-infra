# Operations

## Install and test

```bash
uv sync --frozen
uv run --frozen python -m unittest discover -s tests -v
```

For an installed candidate, use a fresh venv, non-editable wheel and `python -I`; see the round evidence. Do not use the canceled 085 directory as the source of this repository's qualification.

## Private state

`runtime/`, date-scoped policies under `strategy/martin_luk/config/`, research material and reports are local-only. Do not copy them into a public tree or use them as test fixtures. Synthetic tests use `config/synthetic_policy.json` and `tests/fixtures/policies/`.

## Safety

- `live_enabled` must remain false in this release candidate.
- Do not start a real daemon, call provider tools, or modify ledgers/risk/control files.
- `production_ready=false`; formal Host/approval, continuous formal read-only operation and normal real fill protection/exit are unverified.

## Bounded read-only collection

Use the public `martin-agentic bounded-read` entry point described in
[READ_ONLY_COLLECTION.md](READ_ONLY_COLLECTION.md). It is preflight-only by
default; `--execute` is a separate, explicitly approved real read window. No
order/approval tool is exposed by this command.
