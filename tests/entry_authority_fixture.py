"""Test-only synthetic entry authority.

Production `ExecutionBook` always uses DurableEntryAuthority.  Existing tests
that predate the full-account authority still need to create synthetic entries;
this fixture is intentionally test-only and only relaxes the *absence of any
baseline namespace*.  A real APPLIED/REVOKED baseline, durable marker, empty or
partial schema, or read failure remains blocked exactly as production.
"""
from martin_agentic.entry_authority import DurableEntryAuthority, EntryAuthority
from martin_agentic.execution import ExecutionBook


class SyntheticTestEntryAuthority(EntryAuthority):
    def __init__(self, db):
        self.durable = DurableEntryAuthority(db)

    def check(self, *, source, snapshot=None, action=None, purpose=None):
        decision = self.durable.check(
            source=source, snapshot=snapshot, action=action, purpose=purpose)
        if decision.get('reason') == 'ENTRY_AUTHORITY_NAMESPACE_ABSENT':
            return {'allowed': True,
                    'reason': 'SYNTHETIC_TEST_ENTRY_AUTHORITY_ONLY',
                    'source': source}
        return decision


class SyntheticTestPreflight:
    def check_proposal(self, book, snapshot, quote, plan, *, purpose, evidence,
                       candidate_action_id=None):
        return {'allowed': True, 'reasons': [],
                'reason': 'SYNTHETIC_TEST_PREFLIGHT_ONLY',
                'entry_authorized': False}

    def check_claim(self, book, action):
        return {'allowed': True, 'reasons': [],
                'reason': 'SYNTHETIC_TEST_PREFLIGHT_ONLY',
                'entry_authorized': False}


class TestExecutionBook(ExecutionBook):
    """Explicit synthetic fixture; never imported by production modules."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._entry_authority = SyntheticTestEntryAuthority(self.db)
        self._current_preflight = SyntheticTestPreflight()

    def _current_preflight_decision(self, **kwargs):
        if kwargs.get('proposal'):
            return self._current_preflight.check_proposal(
                self, kwargs.get('snapshot'), kwargs.get('quote'),
                kwargs.get('plan'), purpose=kwargs.get('purpose'),
                evidence=kwargs.get('evidence'))
        return self._current_preflight.check_claim(self, kwargs.get('action'))

    def _preflight_binding_ok(self, decision, **kwargs):
        return True
