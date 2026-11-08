"""Shadow research CLI with opt-in authenticated market reads; no broker writes."""

import argparse
from contextlib import ExitStack
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from .ledger import PaperLedger
from .market import inspect_recording
from .paper import PaperDesk
from .shadow import watch_inbox
from .risk import RiskPolicy, number, size_position
from .collector import universe_event
from .runtime_probe import probe_runtime
from .capture import capture_market
from .transport import TransportError
from .protection import ProtectionJournal
from .broker import BrokerJournal, observe_broker
from .trial import trial_proposal
from .audit import AuditTrail
from .review import session_review
from .execution import ExecutionBook
from .handoff import operator_packet
from .qualification import release_check
from .dashboard import serve_dashboard
from .session_control import session_control
from .local_worker_wake import LOCAL_WORKER_ADAPTER, local_worker_wake_adapter
from .runtime_health import observation_healthy
from .agent_worker import CodexOrderWorker, dispatch_one
from .native_operations import NativeOperations
from .capture_archive import export_capture_archive


def _broker_approval_ids(book):
    from .live_session import broker_approval_ids
    return broker_approval_ids(book)


def _session_composition(name, plugin_ids=None):
    if name == 'external':
        from trading_infra.external_application import external_composition
        if not plugin_ids:
            raise ValueError('EXTERNAL_PLUGIN_SELECTION_REQUIRED')
        return external_composition(plugin_ids)
    if name == 'martin':
        from martin_luk.live_session import MARTIN_COMPOSITION
        return MARTIN_COMPOSITION
    if name == 'examples':
        from trading_example_app.composition import COMPOSITION
        return COMPOSITION
    return None


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        # JSON floats are rejected rather than silently rounded at the money boundary.
        return json.load(handle)


def dry_run(scenario):
    if scenario.get("mode") != "paper" or scenario.get("synthetic") is not True:
        raise ValueError("I1 dry-run accepts explicitly synthetic paper scenarios only")
    policy = RiskPolicy(number(scenario["risk_fraction"]),
                        number(scenario["max_position_fraction"]))
    result = size_position(
        equity=number(scenario["equity"]),
        available_buying_power=number(scenario["available_buying_power"]),
        entry_ceiling=number(scenario["entry"]), stop=number(scenario["stop"]),
        cost_buffer_per_share=number(scenario["cost_buffer_per_share"]),
        remaining_portfolio_risk=number(scenario["remaining_portfolio_risk"]),
        policy=policy,
    )
    q = result["quantity"]
    entry, cost = number(scenario["entry"]), number(scenario["cost_buffer_per_share"])
    equity = number(scenario["equity"])
    exits = []
    for item in scenario["exits"]:
        exit_price = number(item["price"])
        if exit_price < 0:
            raise ValueError("Exit price must be nonnegative")
        pnl = q * (exit_price - entry) - q * cost
        exits.append({"name": item["name"], "exit_price": str(exit_price),
                      "pnl_after_assumed_cost": str(pnl), "ending_equity": str(equity + pnl),
                      "account_return": str(pnl / equity)})
    return {"mode": "paper", "synthetic": True, "is_backtest": False,
            "notice": "Independent hypothetical exits; no live data, probability or orders.",
            "sizing": result, "outcomes": exits}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Martin Agentic shadow research and read-only collection")
    sub = parser.add_subparsers(dest="command", required=True)
    plugin = sub.add_parser('plugin', help='Installed external strategy-plugin protocol commands')
    plugin_sub = plugin.add_subparsers(dest='plugin_command', required=True)
    plugin_list = plugin_sub.add_parser('list', help='List installed entry points as JSON')
    plugin_check = plugin_sub.add_parser('check', help='Run contract conformance for one installed plugin')
    plugin_check.add_argument('--plugin', required=True)
    plugin_check.add_argument('--config', help='Flat scalar config JSON file; default {}')
    plugin_backtest = plugin_sub.add_parser('backtest', help='Deterministic synthetic event replay')
    plugin_backtest.add_argument('--plugin', required=True)
    plugin_backtest.add_argument('--dataset', required=True)
    plugin_backtest.add_argument('--config')
    plugin_backtest.add_argument('--output', help='Write the report here as well as stdout')
    plugin_backtest.add_argument('--fee-per-share', default='0.01')
    plugin_backtest.add_argument('--slippage-per-share', default='0.01')
    plugin_backtest.add_argument('--max-notional', default='100')
    plugin_backtest.add_argument('--max-planned-loss', default='2.50')
    plugin_shadow = plugin_sub.add_parser('shadow', help='Offline advisory instance replay; no Book action')
    plugin_shadow.add_argument('--plugin', required=True)
    plugin_shadow.add_argument('--dataset', required=True)
    plugin_shadow.add_argument('--config')
    plugin_shadow.add_argument('--timeout-seconds', type=float, default=30)
    bundle=sub.add_parser('release-bundle',help='Local source/test-input archive and verification; never deploys or runs bundled code')
    bundle.add_argument('mode',choices=['build','verify'])
    bundle.add_argument('--directory',required=True)
    bundle.add_argument('--root',default='.')
    bundle.add_argument('--expected-source')
    bundle.add_argument('--expected-input-set')
    bundle.add_argument('--extract-to')
    local_audit=sub.add_parser('local-audit',help='Private consistent audit snapshots and restore to a new DB; never enables trading')
    local_audit.add_argument('mode',choices=['backup','verify','restore'])
    local_audit.add_argument('--directory',required=True)
    local_audit.add_argument('--db')
    local_audit.add_argument('--destination')
    local_audit.add_argument('--expected-manifest')
    local_audit.add_argument('--timeout-seconds',type=float,default=30)
    lineage=sub.add_parser('session-lineage',help='Explicit account-sidecar registration and prior-session entry gate; no broker writes')
    lineage.add_argument('operation',choices=['status','bootstrap','enroll'])
    lineage.add_argument('--db',required=True)
    lineage.add_argument('--policy',required=True)
    lineage.add_argument('--predecessor-db')
    capture_export = sub.add_parser("capture-export",help="Freeze local consumed events/decisions; never certifies complete market coverage")
    capture_export.add_argument("--db",required=True)
    capture_export.add_argument("--output-dir",required=True)
    capture_export.add_argument("--raw-dir",help="Optional content-addressed raw responses and observation receipts")
    dry = sub.add_parser("dry-run")
    dry.add_argument("--scenario", required=True)
    replay = sub.add_parser("replay-ledger")
    replay.add_argument("--events", required=True)
    replay.add_argument("--db", required=True, help="Local SQLite paper ledger")
    status = sub.add_parser("status")
    status.add_argument("--db", required=True)
    inspect = sub.add_parser("inspect-data", help="Validate read-only Robinhood capture")
    inspect.add_argument("--recording", required=True)
    inspect.add_argument("--as-of", help="Explicit historical replay clock; defaults to actual UTC now")
    inspect.add_argument("--max-quote-age-seconds", type=int, default=15)
    strategy = sub.add_parser("replay-strategy", help="Replay a synthetic research setup")
    strategy.add_argument("--scenario", required=True)
    paper = sub.add_parser("replay-paper", help="Journal explicit simulated execution events")
    paper.add_argument("--events", required=True)
    paper.add_argument("--db", required=True)
    paper_status = sub.add_parser("paper-status")
    paper_status.add_argument("--db", required=True)
    shadow = sub.add_parser("watch-data", help="Monitor local read-only capture files; no network calls")
    shadow.add_argument("--inbox", required=True)
    shadow.add_argument("--db", required=True)
    shadow.add_argument("--once", action="store_true")
    shadow.add_argument("--poll-seconds", type=float, default=5)
    pipeline = sub.add_parser("shadow-replay", help="Atomic end-to-end shadow, no live writes")
    pipeline.add_argument("--events", required=True)
    pipeline.add_argument("--db", required=True)
    pipeline_status = sub.add_parser("shadow-status")
    pipeline_status.add_argument("--db", required=True)
    screen = sub.add_parser("screen-universe")
    screen.add_argument("--input", required=True)
    screen.add_argument("--as-of", required=True)
    bridge=sub.add_parser('recording-universe')
    bridge.add_argument('--recording',required=True)
    bridge.add_argument('--as-of',required=True)
    bridge.add_argument('--universe-id',required=True)
    daemon=sub.add_parser('shadow-daemon')
    daemon.add_argument('--inbox',required=True)
    daemon.add_argument('--db',required=True)
    daemon.add_argument('--once',action='store_true')
    daemon.add_argument('--poll-seconds',type=float,default=5)
    runtime=sub.add_parser('probe-runtime',help='Read-only sanitized CLI readiness check')
    runtime.add_argument('--codex-binary')
    capture = sub.add_parser('capture-market', help='Finite official MCP market reads; no orders')
    capture.add_argument('--requests', required=True)
    capture.add_argument('--output-dir', required=True)
    capture.add_argument('--inbox')
    capture.add_argument('--cycles', type=int, default=1)
    capture.add_argument('--poll-seconds', type=float, default=5)
    capture.add_argument('--codex-binary', default='codex')
    protect = sub.add_parser('replay-protection', help='Synthetic broker protection lifecycle; no orders')
    protect.add_argument('--events', required=True)
    protect.add_argument('--db', required=True)
    protection_status = sub.add_parser('protection-status', help='Inspect protection rehearsal at an explicit clock')
    protection_status.add_argument('--db', required=True)
    protection_status.add_argument('--as-of', required=True)
    broker = sub.add_parser('observe-broker', help='Official account/order/fill reads; never places orders')
    broker.add_argument('--account-suffix', required=True)
    broker.add_argument('--db', required=True)
    broker.add_argument('--cycles', type=int, default=1)
    broker.add_argument('--poll-seconds', type=float, default=5)
    broker_status = sub.add_parser('broker-status', help='Current-clock broker observation health')
    broker_status.add_argument('--db', required=True)
    trial = sub.add_parser('check-trial-proposal', help='Date-scoped risk arithmetic; not order approval')
    trial.add_argument('--policy', required=True)
    trial.add_argument('--input', required=True)
    trial.add_argument('--as-of', help='Historical replay clock only; defaults to actual UTC now')
    audit_record=sub.add_parser('audit-record',help='Persist an action intent/result; does not execute it')
    audit_record.add_argument('--db',required=True)
    audit_record.add_argument('--event',required=True)
    audit_report=sub.add_parser('audit-report',help='Verify chain and export local session JSON/JSONL/Markdown')
    audit_report.add_argument('--db',required=True)
    audit_report.add_argument('--date',required=True)
    audit_report.add_argument('--output-dir',required=True)
    audit_verify=sub.add_parser('audit-verify',help='Verify local audit chain integrity')
    audit_verify.add_argument('--db',required=True)
    review=sub.add_parser('session-review',help='Single post-close review; optionally read broker once')
    review.add_argument('--db',required=True)
    review.add_argument('--date',required=True)
    review.add_argument('--output-dir',required=True)
    review.add_argument('--account-suffix',default=None)
    review.add_argument('--verify-broker',action='store_true')
    live=sub.add_parser('live-session',help='Bounded real-data strategy/lifecycle runner; event-only Agent')
    live.add_argument('--until-close',action='store_true',help='Run only this authorized date until verified close; close-read grace is 30s, never automatic next-day rollover')
    live.add_argument('--db',required=True)
    live.add_argument('--policy',required=True)
    live.add_argument('--symbols',nargs='+',required=True)
    live.add_argument('--cycles',type=int,default=1)
    live.add_argument('--poll-seconds',type=float,default=15)
    live.add_argument('--agent-mode',choices=['disabled','review','script-review','supervised-host','execute'],default='disabled')
    live.add_argument('--worker-adapter',choices=['none',LOCAL_WORKER_ADAPTER],default='none',
        help='Explicit local worker event adapter; default none. local-worker-review is review-only and requires supervised-host.')
    live.add_argument('--status-file',default='runtime/live-status.json')
    live.add_argument('--output-dir',default='runtime/live-agent')
    live.add_argument('--budget-entry-limit',type=int,default=20)
    live.add_argument('--budget-total-limit',type=int,default=40)
    live.add_argument('--entry-inhibit-file')
    live.add_argument('--strategy',choices=['none','examples','martin','external'],default='examples',
        help='Explicit application composition; examples is generic, external selects installed plugins')
    live.add_argument('--strategy-plugin',action='append',default=[],
        help='Installed external entry-point id used by --strategy external (repeatable)')
    live.add_argument('--require-watchdog',action='store_true',help='Require fresh independent guardian proof for new entries; management continues when unavailable')
    bounded=sub.add_parser('bounded-read',help='Public bounded read-only collection; preflight by default, --execute runs one supervised child')
    bounded.add_argument('--policy',required=True)
    bounded.add_argument('--target',required=True)
    bounded.add_argument('--expected-source',required=True)
    bounded.add_argument('--expected-protected',required=True)
    bounded.add_argument('--registry',required=True)
    bounded.add_argument('--symbols',nargs='+')
    bounded.add_argument('--cycles',type=int,default=3)
    bounded.add_argument('--getter-budget',type=int,default=120)
    bounded.add_argument('--wall-seconds',type=float,default=90)
    bounded.add_argument('--execute',action='store_true')
    execution=sub.add_parser('execution-status',help='Inspect durable real lifecycle/outbox, no broker calls')
    execution.add_argument('--db',required=True)
    execution.add_argument('--policy',required=True)
    wake=sub.add_parser('dispatch-action',help='Wake native-tool Agent for one durable action; default review only')
    wake.add_argument('--db',required=True)
    wake.add_argument('--policy',required=True)
    wake.add_argument('--execute',action='store_true')
    wake.add_argument('--output-dir',default='runtime/live-agent')
    packet=sub.add_parser('operator-packet',help='Exact order/protection handoff; never approves or submits')
    packet.add_argument('--db',required=True)
    packet.add_argument('--policy',required=True)
    packet.add_argument('--action-id')
    engineering_exit=sub.add_parser('engineering-exit-request',help='Audit a requested engineering-test close; never cancels or sells')
    engineering_exit.add_argument('--db',required=True)
    engineering_exit.add_argument('--policy',required=True)
    engineering_exit.add_argument('--trade-id',required=True)
    for command in ('native-preview','native-begin','native-record','native-review-run','native-review-recover'):
        native=sub.add_parser(command,help='Supervised official-tool handoff; never calls broker write APIs')
        native.add_argument('--db',required=True)
        native.add_argument('--policy',required=True)
        if command=='native-review-recover':
            native.add_argument('--operation-id',required=True)
            native.add_argument('--receipt-dir',required=True)
        elif command=='native-record':
            native.add_argument('--operation-id',required=True)
            native.add_argument('--response',required=True,help='Actual native tool response JSON, not confirmation')
        else:
            native.add_argument('--action-id',required=True)
            native.add_argument('--snapshot',help='Fresh normalized broker snapshot; omitted reads broker now')
            native.add_argument('--quote',help='Fresh normalized two-sided quote; omitted reads quote now')
            if command=='native-begin':native.add_argument('--operation',choices=['review','place','cancel'],required=True)
            if command=='native-review-run':native.add_argument('--receipt-dir',required=True)
    qualify=sub.add_parser('release-check',help='Report evidence and blockers; never enables trading')
    qualify.add_argument('--db',required=True)
    qualify.add_argument('--policy',required=True)
    qualify.add_argument('--status-file',required=True)
    qualify.add_argument('--test-evidence',required=True)
    control=sub.add_parser('session-control',help='Start/status/stop the bounded review daemon; validates PID identity')
    control.add_argument('mode',choices=['start','status','stop'])
    control.add_argument('--db',required=True)
    control.add_argument('--policy',required=True)
    control.add_argument('--symbols',nargs='+')
    control.add_argument('--agent-mode',choices=['disabled','review','script-review','supervised-host'],default='review')
    control.add_argument('--worker-adapter',choices=['none',LOCAL_WORKER_ADAPTER],default='none',
        help='Explicit local worker event adapter; default none. local-worker-review is review-only and requires supervised-host.')
    control.add_argument('--strategy',choices=['none','examples','martin','external'],default='martin',
        help='Explicit application composition for the child live session')
    control.add_argument('--strategy-plugin',action='append',default=[],
        help='Installed external entry-point id used by --strategy external (repeatable)')
    control.add_argument('--status-file',default='runtime/live-status.json')
    control.add_argument('--metadata-file',default='runtime/live-daemon-control.json')
    dashboard=sub.add_parser('dashboard',help='Loopback-only live dashboard; read-only files/SQLite, no model calls')
    dashboard.add_argument('--db',default='runtime/live.sqlite')
    dashboard.add_argument('--status-file',default='runtime/live-status.json')
    dashboard.add_argument('--capture-dir',default='runtime/live-captures')
    dashboard.add_argument('--release-file',default='reports/production_iteration_evidence.json')
    dashboard.add_argument('--port',type=int,default=8765)
    capabilities=sub.add_parser('capabilities',help='Inspect saved official contracts; never authorizes trading')
    capabilities.add_argument('--inventory',default='reports/community_tool_contract_evidence.json')
    capabilities.add_argument('--requirements',help='Optional JSON boolean capability requirements')
    from .baseline_cli import add_parser as add_baseline_parser
    add_baseline_parser(sub)
    from .broker_bundle import add_parser as add_broker_bundle_parser
    add_broker_bundle_parser(sub)
    script_execution=sub.add_parser('script-execution',help='Read-only shadow or synthetic script-executor rehearsals; no live writes')
    script_execution.add_argument('mode',choices=['shadow','rehearse','rehearse-bounded'])
    script_execution.add_argument('--db')
    script_execution.add_argument('--policy',required=True)
    script_execution.add_argument('--inventory',default='reports/community_tool_contract_evidence.json')
    script_execution.add_argument('--limit',type=int,default=100)
    script_execution.add_argument('--output-dir')
    script_execution.add_argument('--io-timeout-seconds',type=float,default=5)
    recovery=sub.add_parser('recovery-case',help='Audited manual recovery; no broker writes or historical failure clearing')
    recovery.add_argument('mode',choices=['open','status','verify','acknowledge','dispose-protection-incident'])
    recovery.add_argument('--db',required=True)
    recovery.add_argument('--policy',required=True)
    recovery.add_argument('--case-id')
    recovery.add_argument('--trade-id')
    recovery.add_argument('--intent-ids',nargs='*',default=[])
    recovery.add_argument('--incident-order-ids',nargs='*',default=[])
    recovery.add_argument('--manual-order-ids',nargs='*',default=[])
    recovery.add_argument('--execution-ids',nargs='*',default=[])
    recovery.add_argument('--cancelled-order-ids',nargs='*',default=[])
    recovery.add_argument('--reason',default='')
    manual=sub.add_parser('manual-takeover',help='Retrospective exact broker receipt and audited retirement; never enables trading')
    manual.add_argument('mode',choices=['import-response','open','status','verify','acknowledge','dispose-protection-incident'])
    manual.add_argument('--db',required=True)
    manual.add_argument('--policy',required=True)
    manual.add_argument('--case-id')
    manual.add_argument('--trade-id')
    manual.add_argument('--intent-ids',nargs='*',default=[])
    manual.add_argument('--incident-order-ids',nargs='*',default=[])
    manual.add_argument('--manual-order-ids',nargs='*',default=[])
    manual.add_argument('--execution-ids',nargs='*',default=[])
    manual.add_argument('--cancelled-order-ids',nargs='*',default=[])
    manual.add_argument('--reason',default='')
    manual.add_argument('--response',help='Original formal tool response JSON, not an Agent summary')
    manual.add_argument('--account-number')
    manual.add_argument('--tool-call-id')
    manual.add_argument('--origin-reference',help='sha256 of saved original response when the original call ID was not recorded')
    watchdog=sub.add_parser('watchdog',help='Independent script health checks and sticky entry inhibit; no trading')
    watchdog.add_argument('--db',required=True)
    watchdog.add_argument('--status-file',required=True)
    watchdog.add_argument('--policy',help='Required to verify retired manual-recovery history')
    watchdog.add_argument('--cycles',type=int,default=1)
    watchdog.add_argument('--poll-seconds',type=float,default=5)
    watchdog.add_argument('--state-file')
    watchdog.add_argument('--inhibit-file')
    watchdog.add_argument('--stop-on-safe-close',action='store_true')
    watchdog.add_argument('--expected-control-pid',type=int)
    release_gate=sub.add_parser('watchdog-release',help='Fresh broker reconciliation and audited entry-gate release; no trading')
    release_gate.add_argument('--db',required=True)
    release_gate.add_argument('--policy',required=True)
    release_gate.add_argument('--reason',required=True)
    rehearsal=sub.add_parser('fault-rehearsal',help='Offline synthetic execution fault matrix; no broker')
    rehearsal.add_argument('--output-dir',required=True)
    rehearsal.add_argument('--policy')
    research=sub.add_parser('research-replay',help='Registered frozen-strategy cost/scenario replay; no live execution')
    research.add_argument('--manifest',required=True)
    research.add_argument('--output-dir',required=True)
    verify_research=sub.add_parser('research-verify',help='Read-only registered-run reproducibility check; never qualifies profitability or execution')
    verify_research.add_argument('--registry',required=True)
    verify_research.add_argument('--run-id',required=True)
    verify_research.add_argument('--manifest',required=True)
    verify_research.add_argument('--report')
    anchor=sub.add_parser('audit-anchor',help='Export or compare an audit prefix; external storage trust is separate')
    anchor.add_argument('--db',required=True)
    anchor.add_argument('--output')
    anchor.add_argument('--verify')
    custody=sub.add_parser('audit-custody',help='Explicit optional S3 Object Lock audit-prefix custody; default preparation is offline')
    custody.add_argument('mode',choices=['prepare','publish','verify','discover','verify-custodian'])
    custody.add_argument('--db',required=True)
    custody.add_argument('--anchor',required=True)
    custody.add_argument('--config',required=True)
    custody.add_argument('--receipt-dir')
    custody.add_argument('--version-id')
    custody.add_argument('--profile')
    current_evidence=sub.add_parser('current-evidence',help='Read-only current account/product evidence; never authorizes entry')
    current_evidence.add_argument('--bundle-root',required=True,help='Saved official read-only response bundle')
    current_evidence.add_argument('--public-sources',required=True,help='Pinned local official public-source manifest')
    host=sub.add_parser('host-execution',help='Private online Host mailbox; no trading database or broker calls')
    host.add_argument('mode',choices=['status','request','result','receipt','release-gate','engineering-exit','engineering-propose','engineering-intent','engineering-promote'])
    host.add_argument('--directory',required=True)
    host.add_argument('--action-id')
    host.add_argument('--operation',choices=['review','place','cancel'])
    host.add_argument('--request-id')
    host.add_argument('--response',help='Original official tool response JSON; private runtime file, not approval')
    host.add_argument('--reason',help='Audited local gate disposition; cannot approve orders or reset account risk')
    host.add_argument('--trade-id',help='Existing engineering trade; queues local management intent only')
    host.add_argument('--proposal',help='Exact engineering signal_id/plan/evidence JSON; owner supplies fresh market/account facts')
    host.add_argument('--permit-id',help='Exact owner permit id for engineering promotion; never a path or boolean')
    args = parser.parse_args(argv)
    if args.command == 'plugin':
        from trading_infra.plugin_probe import list_plugins_bounded
        try:
            if args.plugin_command == 'list':
                output = {'schema_version': 1, 'plugins': list_plugins_bounded()}
                print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
                return 0
            config = load_json(args.config) if getattr(args, 'config', None) else {}
            if args.plugin_command == 'check':
                from trading_infra.plugin_conformance import run_conformance
                output = run_conformance(args.plugin, config)
            elif args.plugin_command == 'shadow':
                from trading_infra.plugin_shadow import run_shadow
                output = run_shadow(args.plugin, config,
                                    json.loads(Path(args.dataset).read_text()),
                                    timeout_seconds=args.timeout_seconds)
            elif args.plugin_command == 'backtest':
                from trading_infra.plugin_backtest import run_backtest_file
                output = run_backtest_file(
                    args.plugin, config, args.dataset,
                    fee_per_share=args.fee_per_share,
                    slippage_per_share=args.slippage_per_share,
                    max_notional=args.max_notional,
                    max_planned_loss=args.max_planned_loss)
                if args.output:
                    Path(args.output).write_text(
                        json.dumps(output, ensure_ascii=False, indent=2,
                                   sort_keys=True) + '\n')
            else:
                raise ValueError('PLUGIN_COMMAND_INVALID')
        except ValueError as exc:
            code = 3 if 'PLUGIN_NOT_INSTALLED' in str(exc) else 2
            print(json.dumps({'error': str(exc), 'production_ready': False}),
                  file=sys.stderr)
            return code
        print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if output.get('passed') is not False and output.get('error') is None else 2
    if args.command == 'historical-baseline':
        from .baseline_cli import run as run_baseline_cli
        return run_baseline_cli(args)
    if args.command == 'capture-baseline-bundle':
        from .broker_bundle import run_cli as run_broker_bundle_cli
        return run_broker_bundle_cli(args)
    if args.command == 'current-evidence':
        from .current_evidence import run_cli as run_current_evidence_cli
        return run_current_evidence_cli([
            '--bundle-root', args.bundle_root,
            '--public-sources', args.public_sources])
    ledger = None
    ownership = ExitStack()
    try:
        # These legacy offline paths construct mutating book/journal classes.
        # Acquire the controller's lock before any constructor, read RPC or
        # result import; an online owner uses its mailbox/read-only dashboard.
        offline_books={'native-preview','native-begin','native-record',
            'native-review-run','native-review-recover','execution-status',
            'dispatch-action','operator-packet','release-check','watchdog-release',
            'session-lineage'}
        journal_commands={'audit-record','audit-report','audit-verify',
                          'observe-broker','broker-status'}
        research_commands={'status','replay-ledger','replay-paper','paper-status',
            'shadow-replay','shadow-status','replay-protection','protection-status',
            'watch-data','shadow-daemon'}
        guarded=(args.command in offline_books or args.command in journal_commands
                 or args.command=='session-review'
                 or args.command in research_commands-{'watch-data','shadow-daemon'})
        if guarded:
            if args.command in offline_books and not Path(args.db).is_file():
                raise ValueError('Execution database does not exist')
            from .shadow import single_instance
            ownership.enter_context(single_instance(args.db))
        if args.command in research_commands and Path(args.db).is_file():
            # A mistaken paper/status target must never initialize research
            # schemas in an existing execution book, even with no owner running.
            source=Path(args.db).resolve()
            check=sqlite3.connect(source.as_uri()+'?mode=ro',uri=True,timeout=.25)
            try:
                if check.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='live_state'").fetchone():
                    raise ValueError('RESEARCH_COMMAND_REQUIRES_SEPARATE_DATABASE')
            finally:check.close()
        if args.command=='release-bundle':
            from .release_bundle import build_release_bundle,verify_release_bundle
            if args.mode=='build':
                if args.extract_to or args.expected_source or args.expected_input_set:raise ValueError('RELEASE_BUILD_ARGUMENTS_INVALID')
                output=build_release_bundle(args.root,args.directory)
            else:
                if not args.expected_source:raise ValueError('RELEASE_EXPECTED_SOURCE_REQUIRED')
                if not args.expected_input_set:raise ValueError('RELEASE_EXPECTED_INPUT_SET_REQUIRED')
                output=verify_release_bundle(args.directory,expected_source=args.expected_source,
                    expected_input_set_sha256=args.expected_input_set,extract_to=args.extract_to)
        elif args.command=='local-audit':
            from .local_audit_storage import create_local_backup,verify_local_backup,restore_local_backup
            if args.mode=='backup':
                if not args.db or args.destination or args.expected_manifest:raise ValueError('LOCAL_AUDIT_BACKUP_ARGUMENTS_INVALID')
                output=create_local_backup(args.db,args.directory,timeout_seconds=args.timeout_seconds)
            elif args.mode=='verify':
                if args.db or args.destination:raise ValueError('LOCAL_AUDIT_VERIFY_ARGUMENTS_INVALID')
                output=verify_local_backup(args.directory,expected_manifest_sha256=args.expected_manifest)
            else:
                if not args.destination or not args.expected_manifest or args.db:
                    raise ValueError('LOCAL_AUDIT_RESTORE_NEW_DESTINATION_AND_TRUSTED_MANIFEST_REQUIRED')
                output=restore_local_backup(args.directory,args.destination,expected_manifest_sha256=args.expected_manifest)
        elif args.command=='host-execution':
            from .host_mailbox import read_private_json,submit_request,read_request_result,submit_receipt,submit_gate_release,submit_engineering_exit,submit_engineering_proposal,submit_engineering_intent,submit_engineering_promote
            if args.mode=='status':output=read_private_json(Path(args.directory)/'manifest.json')
            elif args.mode=='release-gate':output=submit_gate_release(args.directory,args.reason,request_id=args.request_id)
            elif args.mode=='engineering-exit':output=submit_engineering_exit(args.directory,args.trade_id,request_id=args.request_id)
            elif args.mode=='engineering-propose':
                if not args.proposal:raise ValueError('HOST_ENGINEERING_PROPOSAL_REQUIRED')
                proposal=load_json(args.proposal)
                if not isinstance(proposal,dict) or set(proposal)!={'signal_id','plan','evidence'}:
                    raise ValueError('HOST_ENGINEERING_PROPOSAL_REQUIRED')
                output=submit_engineering_proposal(args.directory,proposal['signal_id'],proposal['plan'],proposal['evidence'],request_id=args.request_id)
            elif args.mode=='engineering-intent':
                if not args.proposal:raise ValueError('HOST_ENGINEERING_INTENT_REQUIRED')
                proposal=load_json(args.proposal)
                if not isinstance(proposal,dict) or set(proposal)!={'signal_id','plan','evidence'}:
                    raise ValueError('HOST_ENGINEERING_INTENT_REQUIRED')
                output=submit_engineering_intent(args.directory,proposal['signal_id'],proposal['plan'],proposal['evidence'],request_id=args.request_id)
            elif args.mode=='engineering-promote':
                if not args.action_id or not args.permit_id:raise ValueError('HOST_ENGINEERING_PROMOTION_REQUIRED')
                output=submit_engineering_promote(args.directory,args.action_id,args.permit_id,request_id=args.request_id)
            elif args.mode=='request':
                if not args.action_id or not args.operation:raise ValueError('HOST_ACTION_AND_OPERATION_REQUIRED')
                output=submit_request(args.directory,args.action_id,args.operation,request_id=args.request_id)
            elif args.mode=='result':
                if not args.request_id:raise ValueError('HOST_REQUEST_ID_REQUIRED')
                output=read_request_result(args.directory,args.request_id)
            else:
                if not args.request_id or not args.response:raise ValueError('HOST_REQUEST_AND_RESPONSE_REQUIRED')
                receipt=submit_receipt(args.directory,args.request_id,read_private_json(Path(args.response)))
                # Original account scope stays private, never printed in CLI output.
                output={'status':'HOST_RECEIPT_PERSISTED','request_id':receipt['request_id'],
                    'response_sha256':receipt['response_sha256'],'execution_authorized':False,'broker_effect_verified':False}
        elif args.command=='script-execution':
            if args.mode in {'rehearse','rehearse-bounded'}:
                if args.db or not args.output_dir:raise ValueError('REHEARSAL_REQUIRES_OUTPUT_DIR_AND_NO_REAL_DB')
                from .script_execution_rehearsal import rehearse_script_execution,rehearse_bounded_entry_protection
                output=(rehearse_script_execution(args.output_dir,load_json(args.policy)) if args.mode=='rehearse'
                    else rehearse_bounded_entry_protection(args.output_dir,load_json(args.policy),io_timeout_seconds=args.io_timeout_seconds))
            else:
                if not args.db or args.output_dir:raise ValueError('SHADOW_REQUIRES_DB_AND_STDOUT_ONLY')
                from .execution_preflight import script_execution_preflight
                output=script_execution_preflight(args.db,load_json(args.policy),load_json(args.inventory),limit=args.limit)
        elif args.command=='capabilities':
            from .capabilities import capability_report,admission_for_capabilities
            output=capability_report(load_json(args.inventory))
            if args.requirements:output['admission']=admission_for_capabilities(output,load_json(args.requirements))
        elif args.command in {'recovery-case','manual-takeover'}:
            from .recovery_control import run_recovery_case
            output=run_recovery_case(args.mode,args.db,load_json(args.policy),
                case_id=args.case_id,trade_id=args.trade_id,intent_ids=args.intent_ids,
                incident_order_ids=args.incident_order_ids,manual_order_ids=args.manual_order_ids,
                execution_ids=args.execution_ids,cancelled_order_ids=args.cancelled_order_ids,
                reason=args.reason,manual_takeover=args.command=='manual-takeover',
                response=load_json(args.response) if getattr(args,'response',None) else None,
                account_number=getattr(args,'account_number',None),
                tool_call_id=getattr(args,'tool_call_id',None),
                origin_reference=getattr(args,'origin_reference',None))
        elif args.command=='watchdog-release':
            from .watchdog import release_entry_inhibit
            from .broker import collect_broker
            from .transport import CodexBrokerReadTransport
            if not Path(args.db).is_file():raise ValueError('Execution database does not exist')
            ledger=ExecutionBook(args.db,load_json(args.policy))
            tracked=sorted(set(ledger.pending_approval_ids())|{r[0] for r in ledger.db.execute('SELECT ref_id FROM external_broker_refs')})
            with CodexBrokerReadTransport() as transport:
                snapshot=collect_broker(transport,account_suffix=ledger.policy['account_suffix'],approval_ids=tracked)
            journal=BrokerJournal(args.db)
            try:journal.ingest(snapshot)
            finally:journal.close()
            output=release_entry_inhibit(ledger,snapshot,reason=args.reason)
        elif args.command=='audit-custody':
            from .audit_custody import prepare_custody,run_custody
            plan=prepare_custody(args.db,load_json(args.anchor),load_json(args.config))
            if args.mode=='prepare':output=plan
            else:
                if not args.receipt_dir:raise ValueError('CUSTODY_RECEIPT_DIRECTORY_REQUIRED')
                if args.mode in {'verify','verify-custodian'} and not args.version_id:raise ValueError('CUSTODY_EXACT_VERSION_REQUIRED')
                output=run_custody(args.mode,plan,args.receipt_dir,version_id=args.version_id,profile=args.profile)
        elif args.command=='audit-anchor':
            from .audit_anchor import export_anchor,verify_anchor
            if bool(args.output)==bool(args.verify):raise ValueError('Require exactly one of --output or --verify')
            output=verify_anchor(args.db,load_json(args.verify)) if args.verify else export_anchor(args.db,args.output)
        elif args.command=='fault-rehearsal':
            from .rehearsal import run_rehearsal
            output=run_rehearsal(args.output_dir,args.policy)
        elif args.command=='research-verify':
            from martin_luk.research_verification import verify_research_run
            output=verify_research_run(args.registry,args.run_id,args.manifest,report_path=args.report)
        elif args.command=='research-replay':
            from martin_luk.research import run_research
            output=run_research(args.manifest,args.output_dir)
        elif args.command=='watchdog':
            from .watchdog import run_watchdog
            output=run_watchdog(args.db,args.status_file,cycles=args.cycles,poll_seconds=args.poll_seconds,
                state_file=args.state_file,inhibit_file=args.inhibit_file,policy=load_json(args.policy) if args.policy else None,
                stop_on_safe_close=args.stop_on_safe_close,expected_control_pid=args.expected_control_pid)
        elif args.command=='dashboard':
            serve_dashboard(args.db,args.status_file,args.capture_dir,args.release_file,port=args.port)
            return 0
        elif args.command=='session-control':
            output=session_control(args.mode,args.db,args.policy,symbols=args.symbols,status_file=args.status_file,
                metadata_file=args.metadata_file,agent_mode=args.agent_mode,
                worker_adapter=args.worker_adapter,strategy=args.strategy,
                strategy_plugins=args.strategy_plugin)
        elif args.command=='live-session':
            from .live_session import run_live_session
            if args.worker_adapter==LOCAL_WORKER_ADAPTER and args.agent_mode!='supervised-host':
                raise ValueError('LOCAL_WORKER_ADAPTER_REQUIRES_SUPERVISED_HOST')
            worker_factory=None
            if args.worker_adapter==LOCAL_WORKER_ADAPTER:
                def worker_factory(book,*,output_dir,policy=None):
                    return local_worker_wake_adapter(book,output_dir=output_dir,
                        policy=policy,review_only=True)
            output=run_live_session(args.db,load_json(args.policy),symbols=args.symbols,cycles=args.cycles,until_close=args.until_close,
                poll_seconds=args.poll_seconds,agent_mode=args.agent_mode,status_file=args.status_file,output_dir=args.output_dir,
                budget_entry_limit=args.budget_entry_limit,budget_total_limit=args.budget_total_limit,entry_inhibit_file=args.entry_inhibit_file,
                require_watchdog=args.require_watchdog,watchdog_policy_path=args.policy,
                host_wake_adapter_factory=worker_factory,
                composition=_session_composition(args.strategy,args.strategy_plugin))
        elif args.command=='engineering-exit-request':
            if not Path(args.db).is_file():raise ValueError('Execution database does not exist')
            from .shadow import single_instance
            from .transport import CodexBrokerReadTransport
            from .broker import collect_broker
            # Standalone compatibility path must own the same lock as the
            # daemon. A running owner uses host-execution engineering-exit.
            with single_instance(args.db):
                ledger=ExecutionBook(args.db,load_json(args.policy))
                try:
                    with CodexBrokerReadTransport() as transport:
                        snapshot=collect_broker(transport,account_suffix=ledger.policy['account_suffix'],approval_ids=_broker_approval_ids(ledger))
                    journal=BrokerJournal(args.db)
                    try:journal.ingest(snapshot)
                    finally:journal.close()
                    output=ledger.request_engineering_exit(args.trade_id,snapshot)
                finally:ledger.close();ledger=None
        elif args.command in {'native-preview','native-begin','native-record','native-review-run','native-review-recover'}:
            if not Path(args.db).is_file():raise ValueError('Execution database does not exist')
            ledger=ExecutionBook(args.db,load_json(args.policy));native=NativeOperations(ledger)
            if args.command=='native-review-recover':
                from .native_review_recovery import recover_native_review
                output=recover_native_review(ledger,args.operation_id,args.receipt_dir)
            elif args.command=='native-record':
                output=native.record(args.operation_id,load_json(args.response))
            else:
                from .broker import collect_broker
                from .transport import CodexBrokerReadTransport,CodexMarketTransport
                from .market import RobinhoodReader
                if args.snapshot:snapshot=load_json(args.snapshot)
                else:
                    with CodexBrokerReadTransport() as transport:
                        snapshot=collect_broker(transport,account_suffix=ledger.policy['account_suffix'],approval_ids=_broker_approval_ids(ledger))
                journal=BrokerJournal(args.db)
                try:journal.ingest(snapshot)
                finally:journal.close()
                action=next((a for a in ledger.actions() if a['id']==args.action_id),None)
                if not action:raise ValueError('Native action does not exist')
                payload=json.loads(action['payload']);quote=load_json(args.quote) if args.quote else None
                if action['kind']!='cancel' and quote is None:
                    with CodexMarketTransport() as transport:
                        recording=RobinhoodReader(transport).fetch('get_equity_quotes',{'symbols':[payload['plan']['symbol']]})
                    normalized=inspect_recording({'schema_version':1,'provider':'robinhood','synthetic':False,'records':[recording]},
                        as_of=datetime.now(timezone.utc).isoformat())
                    quotes=normalized['quotes']
                    if len(quotes)!=1:raise ValueError('Fresh native quote unavailable')
                    quote=quotes[0]
                if args.command=='native-review-run':
                    from .native_review import run_native_review
                    output=run_native_review(ledger,args.action_id,snapshot,quote,args.receipt_dir)
                else:
                    output=native.preview(args.action_id,snapshot,quote=quote) if args.command=='native-preview' else native.begin(args.action_id,args.operation,snapshot,quote=quote)
        elif args.command=='bounded-read':
            from .bounded_read import run_bounded_read
            output=run_bounded_read(
                policy_path=args.policy,target=args.target,
                expected_source=args.expected_source,
                protected_manifest=args.expected_protected,
                registry=args.registry,symbols=args.symbols,cycles=args.cycles,
                getter_budget=args.getter_budget,wall_seconds=args.wall_seconds,
                execute=args.execute)
        elif args.command in {'execution-status','dispatch-action','operator-packet','release-check'}:
            if not Path(args.db).is_file():raise ValueError('Execution database does not exist')
            ledger=ExecutionBook(args.db,load_json(args.policy))
            if args.command=='release-check':output=release_check(ledger,load_json(args.status_file),load_json(args.test_evidence))
            elif args.command=='operator-packet':output=operator_packet(ledger,action_id=args.action_id)
            elif args.command=='execution-status':output=ledger.summary()
            else:
                if args.execute and ledger.policy.get('live_enabled') is not True:
                    raise ValueError('LIVE_WRITE_MODE_REQUIRES_ENABLED_POLICY_AND_NATIVE_CONFIRMATION')
                worker=CodexOrderWorker(Path.cwd(),args.output_dir,review_only=not args.execute)
                if args.execute:
                    # The standalone execution path must enforce the same
                    # production entry scope as the running supervisor.
                    from .production_admission import ProductionAdmission
                    from .qualification import source_fingerprint
                    admission_gate=ProductionAdmission(ledger,agent_mode='execute',owner_source=source_fingerprint())
                    row=ledger.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
                    snapshot=json.loads(row[0]) if row else {}
                    snapshot={k:v for k,v in snapshot.items() if k not in {
                        'reconciliation_issues','new_executions_seen','baseline_established'}}
                    admission=admission_gate.assess(snapshot)
                    entry_allowed=admission_gate.permits_entry_dispatch(admission=admission,allow_entries=True)
                    output=dispatch_one(ledger,worker,allow_entries=entry_allowed)
                    output.update(production_admission=admission,automatic_strategy_entries_blocked=True)
                else:
                    output=dispatch_one(ledger,worker)
        elif args.command=='session-lineage':
            from .session_lineage import lineage_entry_gate,bootstrap_session_lineage,enroll_session_lineage
            if not Path(args.db).is_file():raise ValueError('Execution database does not exist')
            policy=load_json(args.policy);ledger=ExecutionBook(args.db,policy)
            if args.operation=='status':output=lineage_entry_gate(ledger)
            else:
                if args.operation=='enroll' and not args.predecessor_db:raise ValueError('LINEAGE_PREDECESSOR_DB_REQUIRED')
                from .transport import CodexBrokerReadTransport
                from .broker import collect_broker
                tracked=sorted(set(_broker_approval_ids(ledger))|{r[0] for r in ledger.db.execute('SELECT ref_id FROM external_broker_refs')})
                with CodexBrokerReadTransport() as transport:
                    snapshot=collect_broker(transport,account_suffix=policy['account_suffix'],approval_ids=tracked)
                journal=BrokerJournal(args.db)
                try:journal.ingest(snapshot)
                finally:journal.close()
                ledger.reconcile(snapshot,quotes={},features={})
                if args.operation=='bootstrap':
                    output=bootstrap_session_lineage(args.db,policy,current_db=args.db,current_policy=policy)
                else:
                    output=enroll_session_lineage(args.db,policy,predecessor_db=args.predecessor_db,
                        current_db=args.db,current_policy=policy)
        elif args.command=='capture-export':
            output=export_capture_archive(args.db,args.output_dir,raw_dir=args.raw_dir)
        elif args.command == "dry-run":
            output = dry_run(load_json(args.scenario))
        elif args.command=='session-review':
            output=session_review(args.db,session_date=args.date,output_dir=args.output_dir,
                                  account_suffix=args.account_suffix,verify_broker=args.verify_broker)
        elif args.command in {'audit-record','audit-report','audit-verify'}:
            if args.command!='audit-record' and not Path(args.db).is_file():
                raise ValueError('Audit database does not exist')
            ledger=AuditTrail(args.db)
            if args.command=='audit-record':
                output=ledger.append(load_json(args.event))
            elif args.command=='audit-report':
                output=ledger.export(args.date,args.output_dir)
            else:
                output=ledger.verify()
        elif args.command=='probe-runtime':
            output=probe_runtime(args.codex_binary)
        elif args.command=='observe-broker':
            output=observe_broker(args.db,account_suffix=args.account_suffix,cycles=args.cycles,poll_seconds=args.poll_seconds)
        elif args.command=='broker-status':
            if not Path(args.db).is_file():
                raise ValueError('Broker journal does not exist')
            ledger=BrokerJournal(args.db)
            output=ledger.summary(as_of=datetime.now(timezone.utc).isoformat())
        elif args.command=='check-trial-proposal':
            proposal=load_json(args.input)
            output=trial_proposal(load_json(args.policy),proposal['broker'],proposal['quote'],proposal['plan'],
                                 as_of=args.as_of or datetime.now(timezone.utc).isoformat())
        elif args.command=='capture-market':
            output=capture_market(load_json(args.requests), args.output_dir,
                cycles=args.cycles, poll_seconds=args.poll_seconds, inbox=args.inbox,
                binary=args.codex_binary)
        elif args.command in {'replay-protection', 'protection-status'}:
            if args.command == 'protection-status' and not Path(args.db).is_file():
                raise ValueError('Protection journal does not exist')
            ledger = ProtectionJournal(args.db)
            if args.command == 'replay-protection':
                results = [ledger.apply(e) for e in load_json(args.events)]
                current = ledger.snapshot()
                output = {'mode':'protection_rehearsal', 'events':results, 'current':current,
                          'new_risk_blocked':current['new_risk_blocked'], 'live_orders':0}
            else:
                output = ledger.snapshot(as_of=args.as_of)
        elif args.command == "watch-data":
            output = watch_inbox(args.inbox, args.db, once=args.once, poll_seconds=args.poll_seconds)
        elif args.command=='shadow-daemon':
            from martin_luk.service import run_shadow
            output=run_shadow(args.inbox,args.db,once=args.once,poll_seconds=args.poll_seconds)
        elif args.command=='recording-universe':
            output=universe_event(load_json(args.recording),as_of=args.as_of,universe_id=args.universe_id)
        elif args.command == "replay-strategy":
            from martin_luk.strategy import replay_setup
            output = replay_setup(load_json(args.scenario))
        elif args.command == "screen-universe":
            from martin_luk.features import build_candidates
            data=load_json(args.input)
            output=build_candidates(data['histories'],data['indicators'],as_of=args.as_of,universe_id=data['universe_id'])
        elif args.command in {"shadow-replay","shadow-status"}:
            from martin_luk.engine import ShadowEngine
            if args.command=="shadow-status" and not Path(args.db).is_file():
                raise ValueError("Shadow engine database does not exist")
            ledger=ShadowEngine(args.db)
            results=[ledger.apply(e) for e in load_json(args.events)] if args.command=="shadow-replay" else []
            output={"mode":"shadow","events":results,"current":ledger.snapshot(),"live_orders":0}
        elif args.command == "inspect-data":
            output = inspect_recording(load_json(args.recording),
                                       as_of=args.as_of or datetime.now(timezone.utc).isoformat(),
                                       max_quote_age_seconds=args.max_quote_age_seconds)
        elif args.command in {"replay-paper", "paper-status"}:
            if args.command == "paper-status" and not Path(args.db).is_file():
                raise ValueError("Paper desk file does not exist")
            ledger = PaperDesk(args.db)
            results = [ledger.apply(e) for e in load_json(args.events)] if args.command == "replay-paper" else []
            output = {"mode": "paper", "events": results, "current": ledger.snapshot()}
        else:
            if args.command == "status" and not Path(args.db).is_file():
                raise ValueError("Ledger file does not exist")
            ledger = PaperLedger(args.db)
            if args.command == "replay-ledger":
                results = [ledger.apply(event) for event in load_json(args.events)]
                output = {"mode": "paper", "events": results, "current": ledger.snapshot()}
            else:
                output = ledger.snapshot()
        print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
        if args.command=='research-verify':return 0 if output.get('reproducible') is True else 2
        if args.command=='session-lineage' and output.get('blocked') is True:return 2
        if args.command=='script-execution' and output.get('status')=='BLOCKED_CAPABILITY':return 2
        if args.command=='native-review-run' and output.get('status')!='WAITING_USER':return 2
        if args.command=='live-session' and output.get('read_only_capture') is True:
            # An intentionally blocked entry path is not a failed capture.
            # This exit status grants no trading authority; faults still fail.
            return 0 if observation_healthy(output) else 2
        return 2 if output.get("data_usable") is False or output.get('healthy') is False or output.get('blocker') or output.get('new_risk_blocked') is True or output.get('within_trial_limits') is False or output.get('passed') is False or output.get('status') in {'FAILED','DATA_GAP','INCOMPLETE_OPEN_EXPOSURE'} or output.get('admission',{}).get('eligible') is False else 0
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error, TransportError) as exc:
        print(json.dumps({"error": str(exc), "live_order": False}), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    finally:
        try:
            if ledger:
                ledger.close()
        finally:
            ownership.close()
