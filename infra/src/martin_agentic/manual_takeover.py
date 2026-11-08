"""Retrospective manual receipts; never invent pre-submission intent or authority.

Original tool responses are locally supplied evidence, not signed broker records.
Only a fresh, persisted, complete broker observation can corroborate an import.
Acknowledgement retires actions through the existing recovery disposition; their
historical states, incident failures and account risk history stay unchanged.
"""
import hashlib
import json
import re

from .audit import canonical, digest
from .broker import normalize_order
from .native_operations import native_data, response_identity
from .ledger import timestamp
from .recovery import RecoveryCases


class ManualTakeoverCases(RecoveryCases):
    def __init__(self, book):
        super().__init__(book)
        self.db.execute('''CREATE TABLE IF NOT EXISTS manual_takeover_receipts(
            order_id TEXT PRIMARY KEY, trade_id TEXT NOT NULL, payload TEXT NOT NULL)''')
        self.db.commit()

    def import_response(self, trade_id, response, *, account_number, tool_call_id=None, origin_reference=None, reason):
        """Import an exact native ``place_equity_order`` response after the fact.

        ``account_number`` is the original selected request scope; it is hashed
        before persistence. ``tool_call_id`` identifies the original call for
        operator review; no local marker authenticates that call on its own.
        If that identifier was not saved, ``origin_reference`` must pin the saved
        original source as ``sha256:<64 hex>``. It is labelled operator-supplied,
        not fabricated as a historical tool-call identity or authenticated source.
        """
        self.book._begin()
        try:
            state = self.book.state()
            if self.book.simulation or trade_id not in state['trades']:
                raise ValueError('MANUAL_TAKEOVER_REAL_ORIGINAL_TRADE_REQUIRED')
            if (not isinstance(account_number, str) or not account_number.endswith(self.book.policy['account_suffix'])
                    or hashlib.sha256(account_number.encode()).hexdigest() != state.get('account_key')):
                raise ValueError('MANUAL_TAKEOVER_ACCOUNT_MISMATCH')
            if ((tool_call_id is not None and (not isinstance(tool_call_id, str) or not 0 < len(tool_call_id) <= 256))
                    or (origin_reference is not None and (not isinstance(origin_reference, str)
                        or re.fullmatch(r'sha256:[0-9a-f]{64}', origin_reference) is None))
                    or (tool_call_id is None and origin_reference is None)
                    or not isinstance(reason, str) or not reason.strip()):
                raise ValueError('MANUAL_TAKEOVER_CALL_OR_ORIGIN_AND_REASON_REQUIRED')
            data = native_data(response)
            raw = data.get('order')
            if (not isinstance(raw, dict) or data.get('approval') is not None or 'approval_id' in data
                    or ('order_id' in data and data['order_id'] != raw.get('id'))):
                raise ValueError('MANUAL_TAKEOVER_DIRECT_ORDER_RESPONSE_REQUIRED')
            for scope in (data, raw):
                if 'account_number' in scope and scope['account_number'] != account_number:
                    raise ValueError('MANUAL_TAKEOVER_ACCOUNT_MISMATCH')
            order = normalize_order(raw, self.book.clock().isoformat())
            if (order['symbol'] != state['trades'][trade_id]['plan']['symbol']
                    or order['side'] != 'sell'):
                raise ValueError('MANUAL_TAKEOVER_EXIT_TRADE_MISMATCH')
            if self.db.execute('SELECT 1 FROM external_broker_order_ids WHERE order_id=?', (order['id'],)).fetchone():
                raise ValueError('MANUAL_TAKEOVER_OWNER_ORDER_CONFLICT')
            redacted, fingerprint = response_identity(response)
            receipt = {'order_id':order['id'], 'trade_id':trade_id,
                       'account_key':state['account_key'], 'account_mask':'••••'+self.book.policy['account_suffix'],
                       'tool':'place_equity_order', 'tool_call_id':tool_call_id,
                       'original_tool_call_id_recorded':tool_call_id is not None,
                       'origin_reference':origin_reference, 'origin_reference_operator_supplied':origin_reference is not None,
                       'response':redacted, 'response_sha256':fingerprint, 'order':order,
                       'reason':reason, 'retrospective':True, 'pre_submission_intent_recorded':False,
                       'source':'operator_supplied_original_native_response',
                       'broker_effect_verified':False, 'automatic_execution_enabled':False}
            prior = self.db.execute('SELECT payload FROM manual_takeover_receipts WHERE order_id=?', (order['id'],)).fetchone()
            if prior:
                if canonical(json.loads(prior[0])) != canonical(receipt):
                    raise ValueError('MANUAL_TAKEOVER_RECEIPT_CONFLICT')
                self._receipt(order['id'])
                self.db.commit()
                return receipt
            self.db.execute('INSERT INTO manual_takeover_receipts VALUES(?,?,?)',
                            (order['id'],trade_id,canonical(receipt)))
            self.book.audit.append_in_transaction({
                'event_id':'manual-takeover-import-'+order['id'], 'occurred_at':self.book.clock().isoformat(),
                'event_type':'RECONCILIATION_ISSUE', 'actor':'script', 'synthetic':False,
                'account':receipt['account_mask'], 'trade_id':trade_id, 'broker_order_id':order['id'],
                'details':{'manual_takeover':True, 'manual_takeover_receipt_sha256':digest(receipt),
                           'response_sha256':fingerprint, 'retrospective':True,
                           'pre_submission_intent_recorded':False, 'broker_effect_verified':False,
                           'automatic_execution_enabled':False}})
            self.db.commit()
            return receipt
        except BaseException:
            self.db.rollback()
            raise

    def _receipt(self, identity):
        row = self.db.execute('SELECT payload FROM manual_takeover_receipts WHERE order_id=?', (identity,)).fetchone()
        if not row:
            raise ValueError('MANUAL_TAKEOVER_RECEIPT_MISSING')
        receipt = json.loads(row[0])
        event = next((e for e in self._events() if e['event_id']=='manual-takeover-import-'+identity), None)
        if (not event or event['details'].get('manual_takeover_receipt_sha256') != digest(receipt)
                or event.get('synthetic') is not False or receipt.get('retrospective') is not True
                or receipt.get('pre_submission_intent_recorded') is not False):
            raise ValueError('MANUAL_TAKEOVER_RECEIPT_AUDIT_INVALID')
        return receipt

    def _save(self, case):
        if case['status'] == 'OPEN':
            pins = {}
            for identity in case['manual_order_ids']:
                receipt = self._receipt(identity)
                if (receipt['trade_id'] != case['trade_id'] or receipt['account_key'] != case['account_key']):
                    raise ValueError('MANUAL_TAKEOVER_RECEIPT_CASE_MISMATCH')
                pins[identity] = digest(receipt)
            case['manual_receipt_sha256'] = pins
            case['recovery_kind'] = 'retrospective_manual_takeover'
            case['normal_lifecycle_acceptance_passed'] = False
        super()._save(case)

    def _validate_order_linkage(self, case, orders, events, intents, results):
        # The normal incident orders still require their original durable
        # dispatch intent. Only explicit manual IDs use the alternative proof.
        super()._validate_order_linkage({**case, 'manual_order_ids':[]}, orders, events, intents, results)
        incident_orders = [orders[i] for i in case['incident_order_ids']]
        incident_fills = [e for o in incident_orders for e in o['executions']]
        if not incident_fills:
            raise ValueError('MANUAL_TAKEOVER_INCIDENT_FILL_REQUIRED')
        first_created = min(timestamp(o['created_at']) for o in incident_orders)
        first_fill = min(timestamp(e['executed_at']) for e in incident_fills)
        pins = case.get('manual_receipt_sha256', {})
        if set(pins) != set(case['manual_order_ids']):
            raise ValueError('MANUAL_TAKEOVER_RECEIPT_MANIFEST_MISMATCH')
        for identity in case['manual_order_ids']:
            receipt = self._receipt(identity)
            if digest(receipt) != pins[identity]:
                raise ValueError('MANUAL_TAKEOVER_RECEIPT_CHANGED')
            if receipt['account_key'] != case['account_key'] or receipt['trade_id'] != case['trade_id']:
                raise ValueError('MANUAL_TAKEOVER_RECEIPT_CASE_MISMATCH')
            expected, actual = receipt['order'], orders[identity]
            if (timestamp(actual['created_at']) < first_created
                    or any(timestamp(e['executed_at']) < first_fill for e in actual['executions'])):
                raise ValueError('MANUAL_TAKEOVER_EXIT_PREDATES_INCIDENT')
            fields = ('id','symbol','side','type','quantity','stop','limit_price','market_hours','time_in_force','created_at','ref_id')
            if any(expected[k] != actual[k] for k in fields):
                raise ValueError('MANUAL_TAKEOVER_OBSERVED_ORDER_TERMS_MISMATCH')
            # A submitted receipt may precede the fill. Exact observed execution
            # IDs/terms can only extend it, never rewrite already-returned fills.
            observed = {e['id']:e for e in actual['executions']}
            if any(observed.get(e['id']) != e for e in expected['executions']):
                raise ValueError('MANUAL_TAKEOVER_OBSERVED_EXECUTION_MISMATCH')
