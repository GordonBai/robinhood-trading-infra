"""Shared capability evidence consumed by the real admission surfaces."""
import copy
import json
from datetime import datetime, timezone
from unittest import TestCase

from capability_contract_fixture import CONTRACT
from martin_agentic.capabilities import (
    admission_for_capabilities, capability_blockers, capability_report,
    channel_capabilities)

CLOCK = lambda: datetime(2026, 11, 6, 22, 0, tzinfo=timezone.utc)


class CapabilityConsumerTests(TestCase):
    def inventory(self):
        return copy.deepcopy(CONTRACT)

    def test_full_nine_tool_inventory_separates_equity_and_advanced_cancel(self):
        report = capability_report(self.inventory(), clock=CLOCK)
        oco = report['exit_oco']
        self.assertTrue(oco['documented'])
        self.assertTrue(oco['review_present'])
        self.assertTrue(oco['place_present'])
        self.assertTrue(oco['get_present'])
        self.assertTrue(oco['cancel_present'])
        self.assertTrue(oco['advanced_review_explicit_confirmation_when_no_app_approval'])
        self.assertEqual(oco['advanced_cancel_confirmation_statement'],
                         'NOT_STATED')
        self.assertFalse(oco['advanced_cancel_unattended_qualified'])
        self.assertFalse(oco['adapter_live_validated'])
        self.assertTrue(report['equity_cancel_requires_specific_confirmation'])
        self.assertTrue(report['equity_preview_requires_explicit_confirmation_when_no_app_approval'])
        self.assertTrue(report['app_approval_is_not_unattended'])
        self.assertEqual(report['account_trade_approval_setting']['state'],
                         'NOT_READ')
        self.assertFalse(report['unattended_ready'])
        blockers = capability_blockers(report)
        self.assertIn('NO_FORMALLY_QUALIFIED_UNATTENDED_ADAPTER', blockers)
        self.assertIn('CURRENT_NATIVE_PLACE_REQUIRES_SPECIFIC_CONFIRMATION',
                      blockers)
        self.assertIn('CURRENT_NATIVE_CANCEL_REQUIRES_SPECIFIC_CONFIRMATION',
                      blockers)
        self.assertIn('CURRENT_ADVANCED_CANCEL_UNATTENDED_NOT_PROVEN',
                      blockers)
        self.assertIn('ACCOUNT_TRADE_APPROVAL_SETTING_UNKNOWN', blockers)
        admission = admission_for_capabilities(
            report, {'unattended_execution': True})
        self.assertFalse(admission['eligible'])
        self.assertFalse(admission['authorization_granted'])
        matrix = channel_capabilities(self.inventory(), clock=CLOCK)
        self.assertTrue(matrix.supports('equity_cancel'))
        self.assertTrue(matrix.supports('exit_oco_documented'))
        self.assertFalse(matrix.supports('continuous_authorisation'))
        self.assertEqual(matrix.confirmation, 'required')

    def test_synthetic_source_is_rejected_not_treated_as_capability(self):
        data = self.inventory()
        data['source'] = 'synthetic_contract_fixture'
        data['synthetic'] = True
        data['account_data'] = False
        with self.assertRaisesRegex(ValueError, 'OFFICIAL_TOOL_METADATA_REQUIRED'):
            capability_report(data, clock=CLOCK)
        with self.assertRaisesRegex(ValueError, 'OFFICIAL_TOOL_METADATA_REQUIRED'):
            channel_capabilities(data, clock=CLOCK)

    def test_missing_or_unmatched_confirmation_is_unknown_not_false(self):
        read_only = self.inventory()
        read_only['tools'] = [{'name': 'get_equity_quotes',
                               'description': 'Read quotes only.'}]
        report = capability_report(read_only, clock=CLOCK)
        self.assertIsNone(
            report['equity_preview_requires_explicit_confirmation_when_no_app_approval'])
        self.assertIsNone(report['equity_cancel_requires_specific_confirmation'])
        self.assertEqual(report['equity_place_confirmation_requirement'],
                         'UNKNOWN')
        self.assertEqual(report['equity_cancel_confirmation_requirement'],
                         'UNKNOWN')
        self.assertEqual(
            report['exit_oco']['advanced_cancel_confirmation_statement'],
            'UNKNOWN')
        blockers = capability_blockers(report)
        self.assertIn('CURRENT_NATIVE_PLACE_CONFIRMATION_UNKNOWN', blockers)
        self.assertIn('CURRENT_NATIVE_CANCEL_CONFIRMATION_UNKNOWN', blockers)
        self.assertIn('CURRENT_ADVANCED_CANCEL_CONFIRMATION_UNKNOWN', blockers)
        matrix = channel_capabilities(read_only, clock=CLOCK)
        self.assertNotIn('equity_place', matrix.provides)
        self.assertNotIn('equity_review', matrix.provides)
        self.assertNotIn('equity_cancel', matrix.provides)

        changed = self.inventory()
        for tool in changed['tools']:
            if tool['name'] == 'review_equity_order':
                tool['description'] = 'Review an equity order with alerts.'
            if tool['name'] == 'cancel_equity_order':
                tool['description'] = 'Cancel an equity order by order_id.'
        report = capability_report(changed, clock=CLOCK)
        self.assertEqual(report['equity_place_confirmation_requirement'],
                         'UNKNOWN')
        self.assertEqual(report['equity_cancel_confirmation_requirement'],
                         'UNKNOWN')
        blockers = capability_blockers(report)
        self.assertIn('CURRENT_NATIVE_PLACE_CONFIRMATION_UNKNOWN', blockers)
        self.assertIn('CURRENT_NATIVE_CANCEL_CONFIRMATION_UNKNOWN', blockers)
        self.assertNotIn('CURRENT_NATIVE_PLACE_REQUIRES_SPECIFIC_CONFIRMATION',
                         blockers)
        self.assertNotIn('CURRENT_NATIVE_CANCEL_REQUIRES_SPECIFIC_CONFIRMATION',
                         blockers)

    def test_missing_review_does_not_advertise_place(self):
        data = self.inventory()
        data['tools'] = [tool for tool in data['tools']
                         if tool['name'] != 'review_equity_order']
        report = capability_report(data, clock=CLOCK)
        self.assertIsNone(
            report['equity_preview_requires_explicit_confirmation_when_no_app_approval'])
        matrix = channel_capabilities(data, clock=CLOCK)
        self.assertNotIn('equity_review', matrix.provides)
        self.assertNotIn('equity_place', matrix.provides)

    def test_future_unknown_and_partial_metadata_do_not_create_capability(self):
        data = self.inventory()
        data['captured_at'] = '2026-11-07T00:00:00+00:00'
        with self.assertRaisesRegex(ValueError, 'CAPABILITY_METADATA_FUTURE'):
            capability_report(data, clock=CLOCK)

        data = self.inventory()
        data['tools'].append({'name': 'unknown_write_tool',
                              'description': 'place real orders'})
        report = capability_report(data, clock=CLOCK)
        matrix = channel_capabilities(data, clock=CLOCK)
        self.assertNotIn('unknown_write_tool', matrix.provides)
        self.assertFalse(report['unattended_ready'])

        data = self.inventory()
        data['tools'] = [tool for tool in data['tools']
                         if tool['name'] != 'cancel_advanced_order']
        report = capability_report(data, clock=CLOCK)
        self.assertFalse(report['exit_oco']['documented'])
        self.assertIn('CURRENT_ADVANCED_CANCEL_UNATTENDED_NOT_PROVEN',
                      capability_blockers(report))

        data = self.inventory()
        data['tools'].append(data['tools'][0])
        with self.assertRaisesRegex(ValueError, 'TOOL_METADATA_INVALID'):
            capability_report(data, clock=CLOCK)

        data = self.inventory()
        data['tools'][0]['description'] = ''
        with self.assertRaisesRegex(ValueError, 'TOOL_METADATA_INVALID'):
            capability_report(data, clock=CLOCK)
