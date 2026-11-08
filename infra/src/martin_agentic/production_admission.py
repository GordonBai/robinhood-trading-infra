"""Runtime admission for automatic *strategy* entry, never broker authority.

Candidate capture/review and specifically supervised engineering actions are
separate scopes. No formally qualified unattended write adapter or research
admission verifier is registered today. Local files/booleans cannot supply
those missing capabilities, even when software/account diagnostics are green.
Existing-position management must continue while production entry is blocked.
"""
import json
import hashlib
import sqlite3
from pathlib import Path
import threading

from .audit import canonical, digest
from .capabilities import capability_blockers, capability_report
from .qualification import candidate_root, source_files


WRITE_MODES = frozenset({'execute', 'supervised-host'})
MAX_EVIDENCE_BYTES = 2_000_000
JOURNAL_ONLY = {'reconciliation_issues', 'new_executions_seen', 'baseline_established'}
MAX_SOURCE_BYTES = 32_000_000
MAX_SOURCE_FILES = 1000


def _evidence(path):
    """Bounded diagnostic input; reading this document does not authorize it."""
    try:
        path = Path(path)
        if not path.is_file() or path.is_symlink():
            return None
        with path.open('rb') as handle:
            raw = handle.read(MAX_EVIDENCE_BYTES + 1)
        if len(raw) > MAX_EVIDENCE_BYTES:
            return None
        value = json.loads(raw)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, TypeError):
        return None


def _bounded_source_fingerprint():
    """Same release identity, with explicit inventory/byte limits before reads."""
    root = candidate_root()
    files = source_files(root)
    if len(files) > MAX_SOURCE_FILES:
        raise ValueError('PRODUCTION_SOURCE_INVENTORY_LIMIT')
    total = sum(path.stat().st_size for path in files)
    if total > MAX_SOURCE_BYTES:
        raise ValueError('PRODUCTION_SOURCE_BYTES_LIMIT')
    value = hashlib.sha256(b'martin-source-v2\0')
    remaining = MAX_SOURCE_BYTES
    for path in files:
        with path.open('rb') as handle:
            raw = handle.read(remaining + 1)
        remaining -= len(raw)
        if remaining < 0:
            raise ValueError('PRODUCTION_SOURCE_CHANGED_OR_OVERSIZED')
        value.update(path.relative_to(root).as_posix().encode());value.update(b'\0')
        value.update(raw);value.update(b'\0')
    return value.hexdigest()


class ProductionAdmission:
    def __init__(self, book, *, agent_mode, owner_source, test_evidence_path=None,
                 research_evidence_path=None, capability_inventory_path=None):
        if agent_mode not in {'disabled', 'review', 'script-review', 'supervised-host', 'execute'}:
            raise ValueError('PRODUCTION_ADMISSION_MODE_INVALID')
        self.book = book
        self.agent_mode = agent_mode
        self.owner_source = owner_source
        self.owner_thread = threading.get_ident()
        root = candidate_root()
        self.test_evidence_path = test_evidence_path or root/'reports/current_build_test_evidence.json'
        self.research_evidence_path = research_evidence_path or root/'reports/community-research/latest-evidence.json'
        # A saved local tool-contract inventory is diagnostic evidence only.
        # No path means the capability source is explicitly unavailable; it
        # never becomes a permissive default.
        self.capability_inventory_path = capability_inventory_path

    def _capability_report(self):
        if not self.capability_inventory_path:
            return None
        raw = _evidence(self.capability_inventory_path)
        if raw is None:
            return None
        try:
            return capability_report(raw, clock=self.book.clock)
        except (ValueError, KeyError, TypeError, OSError):
            return None

    def assess(self, snapshot):
        required = self.book.simulation is False and self.agent_mode in WRITE_MODES
        capability = self._capability_report()
        result = {'scope':'automatic_strategy_entry', 'required_for_current_dispatch':required,
            'agent_mode':self.agent_mode, 'automatic_strategy_entry_allowed':False,
            'execution_authorized':False, 'formal_adapter_qualified':False,
            'research_admitted':False, 'local_evidence_is_not_authority':True,
            'engineering_scope_unchanged':True, 'management_scope_unchanged':True,
            'capability_evidence': {
                'available': capability is not None,
                'metadata_sha256': capability.get('metadata_sha256') if capability else None,
                'unattended_ready': capability.get('unattended_ready') if capability else None},
            'blockers': ['STRATEGY_RESEARCH_ADMISSION_NOT_ESTABLISHED']}
        result['blockers'].extend(capability_blockers(
            capability,
            unattended_code='FORMALLY_QUALIFIED_UNATTENDED_ADAPTER_UNAVAILABLE'))
        if threading.get_ident() != self.owner_thread:
            result['blockers'].append('PRODUCTION_ADMISSION_OWNER_THREAD_REQUIRED')
            return result
        # Simulation/review scope is not relabelled a live admission. These
        # diagnostics need not prevent historical capture/candidate evaluation.
        if self.book.simulation is not False:
            result['blockers'].append('SYNTHETIC_OR_UNKNOWN_BOOK_NOT_PRODUCTION')
            return result
        if not required:
            result['diagnostic_scope']='REVIEW_OR_CAPTURE_NOT_A_PRODUCTION_REQUEST'
            return result
        blockers = result['blockers']
        try:
            current_source = _bounded_source_fingerprint()
        except (OSError, ValueError):
            current_source = None
            blockers.append('PRODUCTION_SOURCE_IDENTITY_UNAVAILABLE')
        result['current_source_fingerprint'] = current_source
        result['owner_source_matches'] = current_source is not None and self.owner_source == current_source
        if not result['owner_source_matches']:
            blockers.append('PRODUCTION_OWNER_SOURCE_CHANGED')
        tests = _evidence(self.test_evidence_path)
        result['software_test_evidence_current'] = bool(current_source is not None and tests
            and tests.get('passed') is True
            and tests.get('source_fingerprint') == current_source
            and tests.get('source_unchanged_during_tests') is True
            and type(tests.get('tests')) is int and tests['tests'] > 0)
        if not result['software_test_evidence_current']:
            blockers.append('PRODUCTION_CURRENT_SOURCE_TEST_EVIDENCE_MISSING')
        research = _evidence(self.research_evidence_path)
        result['research_diagnostic_sha256'] = digest(research) if research else None
        # Existing research deliberately reports simulated executions and no
        # admitted strategy edge. A caller-written profitability/ready flag is
        # not a registered reproducible admission verifier or a provider proof.
        result['research_diagnostic_available'] = research is not None
        if not research:
            blockers.append('PRODUCTION_RESEARCH_DIAGNOSTIC_MISSING')
        try:
            now = self.book.clock()
            self.book._snapshot_check(snapshot, now)
            state = self.book.state()
            if (not state.get('account_key') or state['account_key'] != snapshot.get('account_key')
                    or snapshot.get('account_mask') != '••••'+self.book.policy['account_suffix']):
                raise ValueError('account binding')
            current = self.book.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
            health = self.book.db.execute('SELECT payload FROM broker_health WHERE id=1').fetchone()
            current, health = json.loads(current[0]) if current else {}, json.loads(health[0]) if health else {}
            if (health.get('healthy') is not True or health.get('at') != snapshot['observed_at']
                    or current.get('reconciliation_issues')
                    or canonical({k:v for k,v in current.items() if k not in JOURNAL_ONLY}) != canonical(snapshot)):
                raise ValueError('latest complete journal')
            result['current_account_observation_verified'] = True
        except (ValueError, KeyError, TypeError, AttributeError, sqlite3.Error):
            result['current_account_observation_verified'] = False
            blockers.append('PRODUCTION_CURRENT_BOUND_ACCOUNT_NOT_VERIFIED')
        try:
            gate = self.book.entry_gate()
            result['lineage_entry_gate_clear'] = gate.get('blocked') is False
            if not result['lineage_entry_gate_clear']:
                blockers.append('PRODUCTION_LINEAGE_OR_ENTRY_INHIBIT_BLOCKED')
            risk = self.book.risk_guard.state()
            result['current_risk_verified'] = bool(risk and risk.get('blocked') is False
                and risk.get('account_key') == snapshot.get('account_key')
                and risk.get('snapshot_hash') == digest(snapshot))
            if not result['current_risk_verified']:
                blockers.append('PRODUCTION_CURRENT_ACCOUNT_RISK_NOT_VERIFIED')
            if any(a['state'] in {'RUNNING', 'UNKNOWN'} for a in self.book.actions()):
                blockers.append('PRODUCTION_UNRESOLVED_WRITE_OR_ACTION')
            if self.book.active_issues(snapshot):
                blockers.append('PRODUCTION_ACTIVE_LIFECYCLE_OR_RISK_ISSUES')
            if self.book.audit.verify().get('integrity_valid') is not True:
                blockers.append('PRODUCTION_AUDIT_INTEGRITY_NOT_VERIFIED')
        except (ValueError, KeyError, TypeError, AttributeError, sqlite3.Error):
            blockers.append('PRODUCTION_RUNTIME_STATE_NOT_VERIFIED')
        if self.book.policy.get('live_enabled') is not True:
            blockers.append('PRODUCTION_POLICY_LIVE_DISABLED')
        result['blockers'] = sorted(set(blockers))
        return result

    def permits_entry_dispatch(self, *, admission, allow_entries):
        """Blocked production never dispatches a pre-existing strategy entry.

        The Host may still accept a new specifically parameterized engineering
        request when no strategy entry is dispatchable. Neither this scope nor
        a production report supplies the native tool's confirmation.
        """
        if not allow_entries:
            return False
        # Re-derive the enforced scope; a caller-mutated diagnostic dictionary
        # cannot turn unavailable formal capability into permission.
        if self.book.simulation is not False or self.agent_mode not in WRITE_MODES:
            return True
        for action in self.book.actions():
            if action['kind'] != 'entry' or action['state'] not in {'READY','WAITING_USER','RUNNING','UNKNOWN','REPORTED'}:
                continue
            try:
                purpose = json.loads(action['payload']).get('purpose', 'strategy')
            except (ValueError, TypeError):
                return False
            if purpose != 'engineering':
                return False
        return True
