import json
from pathlib import Path
from unittest import TestCase
from martin_agentic.capabilities import (admission_for_capabilities, capability_report,
                                         channel_capabilities)


class CapabilityTests(TestCase):
    def inventory(self):return json.loads(Path('reports/community_tool_contract_evidence.json').read_text())
    def test_current_contract_distinguishes_oco_bracket_confirmation_and_gateway(self):
        r=capability_report(self.inventory())
        self.assertTrue(r['exit_oco']['documented'])
        self.assertTrue(r['equity_preview_requires_explicit_confirmation_when_no_app_approval'])
        self.assertTrue(r['equity_cancel_requires_specific_confirmation'])
        self.assertFalse(r['exit_oco']['entry_atomic_bracket_proven'])
        self.assertFalse(r['unattended_ready']);self.assertFalse(r['write_boundary']['project_gateway_globally_enforced'])
        self.assertEqual(r['account_trade_approval_setting']['state'],'NOT_READ')
    def test_documented_oco_is_not_live_adapter_acceptance(self):
        r=capability_report(self.inventory())
        a=admission_for_capabilities(r,{'exit_oco_live':True,'atomic_entry_bracket':True,'unattended_execution':True})
        self.assertFalse(a['eligible']);self.assertEqual(len(a['blockers']),3)
        self.assertFalse(a['authorization_granted'])
    def test_missing_or_duplicate_metadata_cannot_establish_capability(self):
        data=self.inventory();data['tools'].append(data['tools'][0])
        with self.assertRaisesRegex(ValueError,'INVALID'):capability_report(data)
        data=self.inventory();data['tools']=[t for t in data['tools'] if 'advanced' not in t['name']]
        self.assertFalse(capability_report(data)['exit_oco']['documented'])
    def test_unknown_requirement_fails_closed_and_nonboolean_rejected(self):
        r=capability_report(self.inventory())
        self.assertFalse(admission_for_capabilities(r,{'unknown':True})['eligible'])
        with self.assertRaises(ValueError):admission_for_capabilities(r,{'unattended_execution':'yes'})

    def test_channel_matrix_advertises_only_evidenced_tokens(self):
        matrix = channel_capabilities(self.inventory())
        provides = set(matrix.provides)
        self.assertIn('equity_review', provides)
        self.assertIn('equity_place', provides)
        # This inventory carries no market-data tools, so no data capability is
        # advertised and a strategy needing quotes would be refused.
        self.assertNotIn('quotes', provides)
        # The current contract has no proven unattended execution.
        self.assertNotIn('continuous_authorisation', provides)
        self.assertFalse(matrix.continuous_authorisation)
        self.assertEqual(matrix.confirmation, 'required')
        self.assertFalse(matrix.synthetic)

    def test_a_requirement_the_channel_lacks_is_refused_at_negotiation(self):
        matrix = channel_capabilities(self.inventory())
        with self.assertRaises(ValueError) as ctx:
            matrix.require(['equity_review', 'continuous_authorisation'])
        self.assertEqual(str(ctx.exception),
                         'CAPABILITY_NOT_PROVIDED:continuous_authorisation')

    def test_the_matrix_agrees_with_the_legacy_admission_view(self):
        report = capability_report(self.inventory())
        legacy = admission_for_capabilities(report, {'unattended_execution': True})
        matrix = channel_capabilities(self.inventory())
        self.assertFalse(legacy['eligible'])
        self.assertFalse(matrix.supports('continuous_authorisation'))
