"""Bind the concrete provider surfaces to the declared infrastructure ports.

The ports in ``trading_contracts.ports`` describe powers, not implementations.
These adapters are the composition layer that gives each power a real object, so
the infrastructure can be written against the port and only this module knows a
Robinhood tool name.

Every adapter is a thin delegation. It adds no retries, no fallbacks and no
confirmation bypass: if the wrapped surface raises, the adapter raises.
"""
from dataclasses import dataclass


class MarketDataPortAdapter:
    """Read-only market observations over the official read tools."""

    QUOTES = 'get_equity_quotes'
    HISTORICALS = 'get_equity_historicals'

    def __init__(self, reader):
        for name in ('fetch',):
            if not callable(getattr(reader, name, None)):
                raise ValueError('ADAPTER_READER_REQUIRED')
        self.reader = reader

    def quotes(self, symbols, *, as_of=None):
        """Current two-sided quotes. Freshness is the caller's obligation."""
        record = self.reader.fetch(self.QUOTES, {'symbols': list(symbols)})
        return record['response']['data']

    def bars(self, symbols, *, interval, start, end):
        return self.reader.fetch(self.HISTORICALS, {
            'symbols': list(symbols), 'interval': interval, 'bounds': 'regular',
            'adjustment_type': 'split', 'start_time': start, 'end_time': end,
        })['response']['data']


class BrokerReadPortAdapter:
    """Positions and orders, readable only."""

    def __init__(self, journal, *, reader=None):
        if not callable(getattr(journal, 'summary', None)):
            raise ValueError('ADAPTER_JOURNAL_REQUIRED')
        self.journal = journal
        self.reader = reader

    def positions(self):
        return (self.journal.summary() or {}).get('positions', {})

    def orders(self):
        return int((self.journal.summary() or {}).get('open_equity_orders', 0))

    def snapshot(self):
        """The full account-bound observation, or an explicit refusal.

        A reader that cannot supply it must not be used as the account
        authority: a count is not the order set and a summary has no observation
        time of its own.
        """
        reader = getattr(self.reader, 'snapshot', None)
        if not callable(reader):
            raise ValueError('ADAPTER_SNAPSHOT_UNAVAILABLE')
        return reader.snapshot()


class ExecutionPortAdapter:
    """Two distinct powers: advancing the queue, and acting on one intent.

    Conflating them is how ``cancel(A)`` could end up servicing whatever the
    dispatcher had queued next. Queue advancement is offered separately and
    never pretends to be a targeted operation; a targeted operation requires an
    adapter that receives the target.
    """

    def __init__(self, *, advance=None, operate=None):
        if not callable(advance) and not callable(operate):
            raise ValueError('ADAPTER_EXECUTION_CALLABLE_REQUIRED')
        self.advance = advance
        self.operate = operate

    # -- queue advancement (no target) -----------------------------------
    def advance_queue(self, *, allow_entries=False):
        if type(allow_entries) is not bool:
            raise ValueError('ADAPTER_ENTRY_FLAG_INVALID')
        if not callable(self.advance):
            raise ValueError('ADAPTER_QUEUE_ADVANCE_UNAVAILABLE')
        return self.advance(allow_entries=allow_entries)

    # -- targeted operations ---------------------------------------------
    def _targeted(self, action_id, operation, intent):
        if not isinstance(action_id, str) or not action_id or len(action_id) > 128:
            raise ValueError('ADAPTER_ACTION_ID_REQUIRED')
        if not callable(self.operate):
            # Refusing is the only honest answer: without a target-aware
            # callable this adapter cannot promise the operation applies to
            # this intent and to nothing else.
            raise ValueError('ADAPTER_TARGETED_OPERATION_REQUIRED')
        return self.operate(action_id, operation, intent)

    def review(self, intent):
        if not isinstance(intent, dict):
            raise ValueError('ADAPTER_ENTRY_INTENT_REQUIRED')
        return self._targeted(intent.get('action_id'), 'review', intent)

    def place(self, intent):
        if not isinstance(intent, dict) or intent.get('allow_entries') is not True:
            raise ValueError('ADAPTER_ENTRY_INTENT_REQUIRED')
        return self._targeted(intent.get('action_id'), 'place', intent)

    def cancel(self, ref_id):
        if not isinstance(ref_id, str) or not ref_id:
            raise ValueError('ADAPTER_CANCEL_REF_REQUIRED')
        return self._targeted(ref_id, 'cancel', {'action_id': ref_id})


class HostDeliveryPortAdapter:
    """Deliver one named intent to the host and read its receipt."""

    def __init__(self, bridge, *, inbox):
        if not callable(bridge) or not callable(getattr(inbox, 'get', None)):
            raise ValueError('ADAPTER_HOST_BRIDGE_REQUIRED')
        self.bridge = bridge
        self.inbox = inbox

    def deliver(self, intent):
        if not isinstance(intent, dict):
            raise ValueError('ADAPTER_ENTRY_INTENT_REQUIRED')
        intent_id = intent.get('intent_id')
        if not isinstance(intent_id, str) or not intent_id or len(intent_id) > 128:
            raise ValueError('ADAPTER_INTENT_ID_REQUIRED')
        # The identity travels with the delivery, so the host can bind its
        # confirmation and receipt to this intent and not to another one.
        return self.bridge(intent_id, intent)

    def receipt(self, intent_id):
        if not isinstance(intent_id, str) or not intent_id:
            raise ValueError('ADAPTER_INTENT_ID_REQUIRED')
        return self.inbox.get(intent_id)


@dataclass(frozen=True)
class AdapterBundle:
    """The concrete objects a session assembles, exposed as ports."""
    market: object
    broker: object
    execution: object = None
    host: object = None

    def as_dict(self):
        return {'market': type(self.market).__name__,
                'broker': type(self.broker).__name__,
                'execution': type(self.execution).__name__ if self.execution else None,
                'host': type(self.host).__name__ if self.host else None}


class ProviderBrokerReadPort:
    """The default account authority, addressed only through ``snapshot()``.

    ``read`` is a composition-layer callable returning the complete account
    observation. The port refuses a partial snapshot instead of letting a
    summary stand in for orders, approvals and observation time.
    """

    REQUIRED = ('account_key', 'observed_at', 'orders', 'collection_complete')

    def __init__(self, read):
        if not callable(read):
            raise ValueError('ADAPTER_BROKER_READ_REQUIRED')
        self.read = read

    def positions(self):
        return (self.snapshot() or {}).get('positions', {})

    def orders(self):
        return len((self.snapshot() or {}).get('orders') or {})

    def snapshot(self, *args, **kwargs):
        value = self.read(*args, **kwargs)
        if not isinstance(value, dict):
            raise ValueError('BROKER_SNAPSHOT_REQUIRED')
        missing = [key for key in self.REQUIRED
                   if key not in value or value[key] in (None, '')]
        if missing or value.get('collection_complete') is not True:
            raise ValueError('BROKER_SNAPSHOT_INCOMPLETE:' + ','.join(missing))
        return value


class ProviderMarketDataPort:
    """The default market collection, parameterised by plugin requirements."""

    def __init__(self, collect):
        if not callable(collect):
            raise ValueError('ADAPTER_MARKET_COLLECT_REQUIRED')
        self.collect = collect

    def collect_events(self, signals, *, symbols, requirements, clock,
                       capture_context, cancellation):
        return self.collect(signals, symbols=symbols, requirements=requirements,
                            clock=clock, capture_context=capture_context,
                            cancellation=cancellation)


class ProviderExecutionPort:
    """Default execution power: queue advance when a dispatcher exists.

    Targeted review/place/cancel are a different power. Without an explicit
    target-aware callable this port refuses; it never falls back to the global
    queue, because "advance whatever is next" is not "act on this action".
    """

    def __init__(self, *, advance=None, operate=None):
        self.advance = advance
        self.operate = operate

    def advance_queue(self, *, allow_entries=False):
        if type(allow_entries) is not bool:
            raise ValueError('ADAPTER_ENTRY_FLAG_INVALID')
        if not callable(self.advance):
            raise ValueError('ADAPTER_QUEUE_ADVANCE_UNAVAILABLE')
        return self.advance(allow_entries=allow_entries)

    def _targeted(self, operation, intent):
        if not callable(self.operate):
            raise ValueError('ADAPTER_TARGETED_OPERATION_REQUIRED')
        return self.operate(intent.get('action_id'), operation, intent)

    def review(self, intent):
        return self._targeted('review', intent)

    def place(self, intent):
        return self._targeted('place', intent)

    def cancel(self, ref_id):
        return self._targeted('cancel', {'action_id': ref_id})


class BlockedHostDeliveryPort:
    """A host surface without targeted delivery, refused explicitly."""

    available = False

    def __init__(self, reason):
        self.reason = reason

    def deliver(self, intent):
        raise ValueError(self.reason)

    def receipt(self, intent_id):
        raise ValueError(self.reason)


class BridgeHostDeliveryPort:
    """Composition adapter: bridge request files as targeted host delivery.

    The bridge's own per-action packet flow stays authoritative; this adapter
    only adds an exact-intent request and reads its receipt back by the same
    identity. A missing, late or ambiguous receipt is ``None``, never inferred
    from "whatever the bridge is currently waiting on".
    """

    available = True

    def __init__(self, bridge):
        for name in ('binding', 'directory', 'book'):
            if not hasattr(bridge, name):
                raise ValueError('HOST_TARGETED_DELIVERY_UNAVAILABLE')
        if not hasattr(bridge, 'clock') and not hasattr(bridge.book, 'clock'):
            raise ValueError('HOST_TARGETED_DELIVERY_UNAVAILABLE')
        self.bridge = bridge
        self.directory = __import__('pathlib').Path(bridge.directory)

    @staticmethod
    def request_id(intent_id, operation='review'):
        import uuid
        key = 'host-delivery:' + str(intent_id)
        if operation != 'review':
            # The same action legitimately has a review then a place/cancel
            # operation. Each operation needs its own request identity; the
            # legacy review id stays stable for existing receipts.
            key += ':' + str(operation)
        return str(uuid.uuid5(uuid.NAMESPACE_URL, key))

    def _request_path(self, request_id):
        return self.directory / 'requests' / (request_id + '.json')

    def _index_path(self, intent_id):
        import uuid
        return self.directory / 'deliveries' / (
            str(uuid.uuid5(uuid.NAMESPACE_URL,
                           'host-delivery-index:' + str(intent_id))) + '.json')

    def _read_index(self, intent_id):
        import json
        path = self._index_path(intent_id)
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text())
        except (OSError, ValueError):
            raise ValueError('HOST_DELIVERY_INDEX_UNREADABLE') from None
        if (not isinstance(value, dict)
                or value.get('intent_id') != intent_id
                or value.get('operation') not in {'review', 'place', 'cancel'}
                or not isinstance(value.get('request_id'), str)
                or not value.get('request_id')
                or not isinstance(value.get('action_id'), str)
                or not value.get('action_id')
                or not isinstance(value.get('trade_id'), str)
                or not value.get('trade_id')):
            raise ValueError('HOST_DELIVERY_INDEX_INVALID')
        # The request id is a function of the intent and operation. Checking
        # both against separate allowlists is not enough: a place index that
        # still points at the review request is a different identity.
        if value['request_id'] != self.request_id(intent_id,
                                                  value['operation']):
            raise ValueError('HOST_DELIVERY_INDEX_IDENTITY_MISMATCH')
        return value

    def _legacy_diagnostic(self, status, error):
        import json
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            (self.directory / 'legacy-identity-diagnostic.json').write_text(
                json.dumps({'status': status, 'error': error}, sort_keys=True))
        except OSError:
            pass

    def legacy_management_deliveries(self):
        """Return old entry-intent-keyed management deliveries needing review.

        Before action-scoped identities, protect/cancel/exit requests reused the
        entry execution intent. A new dispatcher must not look up only the new
        action identity and treat the old in-flight request as never sent.
        The old files are left intact; dispatch is blocked until reconciliation.
        An unreadable, malformed, or unmappable delivery index is *not* proof
        that no old request exists, so it raises and leaves a durable diagnostic.
        """
        import json
        folder = self.directory / 'deliveries'
        if not folder.exists():
            return []
        actions = {action['id']: action for action in self.bridge.book.actions()}
        result = []
        for path in sorted(folder.glob('*.json')):
            try:
                value = json.loads(path.read_text())
            except (OSError, ValueError):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'unreadable_delivery_index')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED') from None
            if not isinstance(value, dict):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_index_not_object')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            required = ('action_id', 'trade_id', 'intent_id', 'operation',
                        'request_id')
            if any(not isinstance(value.get(key), str) or not value.get(key)
                   for key in required):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_index_shape_invalid')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            action_id = value['action_id']
            action = actions.get(action_id)
            if action is None:
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_ACTION_UNKNOWN',
                                        action_id[:128])
                raise ValueError('LEGACY_HOST_IDENTITY_ACTION_UNKNOWN')
            if value['trade_id'] != action.get('trade_id'):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_index_trade_mismatch')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            if value['operation'] not in {'review', 'place', 'cancel'}:
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_index_operation_invalid')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            request_path = self.directory / 'requests' / (
                value['request_id'] + '.json')
            try:
                request = json.loads(request_path.read_text())
            except (OSError, ValueError):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_request_missing_or_unreadable')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED') from None
            if (not isinstance(request, dict)
                    or any(request.get(key) != value.get(key) for key in
                           ('intent_id', 'request_id', 'operation',
                            'action_id', 'trade_id'))):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_request_binding_invalid')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            if action.get('kind') == 'entry':
                continue
            if action.get('state') not in {'READY', 'RUNNING', 'UNKNOWN',
                                           'WAITING_USER', 'REPORTED'}:
                continue
            intent_id = value['intent_id']
            if intent_id.startswith('action-'):
                if path != self._index_path(intent_id):
                    self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                            'delivery_index_path_mismatch')
                    raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
                if value['request_id'] != self.request_id(
                        intent_id, value['operation']):
                    self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                            'delivery_request_identity_mismatch')
                    raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            if intent_id == 'action-' + action['id']:
                continue
            prefix = 'action-' + action['id'] + '-review-refresh-'
            if intent_id.startswith(prefix):
                operation_id = intent_id[len(prefix):]
                if not operation_id:
                    self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                            'refresh_identity_operation_missing')
                    raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
                try:
                    row = self.bridge.book.db.execute(
                        "SELECT action_id,operation FROM native_operation_records"
                        " WHERE id=?", (operation_id,)).fetchone()
                except (AttributeError, sqlite3.Error):
                    row = None
                if row != (action['id'], 'review') or value['operation'] != 'review':
                    self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                            'refresh_identity_unbound')
                    raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
                continue
            if not intent_id.startswith('intent-'):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_index_intent_unrecognized')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            try:
                row = self.bridge.book.db.execute(
                    'SELECT trade_id FROM execution_intents WHERE intent_id=?',
                    (intent_id,)).fetchone()
            except (AttributeError, sqlite3.Error):
                row = None
            if row is None or row[0] != action.get('trade_id'):
                self._legacy_diagnostic('LEGACY_HOST_IDENTITY_SCAN_FAILED',
                                        'delivery_index_intent_unbound')
                raise ValueError('LEGACY_HOST_IDENTITY_SCAN_FAILED')
            result.append({'action_id': action['id'],
                           'intent_id': intent_id,
                           'operation': value['operation'],
                           'request_id': value['request_id']})
        if result:
            self._legacy_diagnostic('LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION',
                                    '|'.join(sorted(item['action_id'] for item in result)))
        return result

    def _index_for_request(self, request_id):
        import json
        folder = self.directory / 'deliveries'
        if not folder.exists():
            return None
        for path in sorted(folder.glob('*.json')):
            try:
                value = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if isinstance(value, dict) and value.get('request_id') == request_id:
                return value
        return None

    def _load_request(self, request_id):
        import json
        path = self._request_path(str(request_id))
        if not path.exists():
            raise ValueError('HOST_DELIVERY_REQUEST_MISSING')
        try:
            request = json.loads(path.read_text())
        except (OSError, ValueError):
            raise ValueError('HOST_DELIVERY_REQUEST_UNREADABLE') from None
        if not isinstance(request, dict):
            raise ValueError('HOST_DELIVERY_REQUEST_IDENTITY_MISMATCH')
        return request

    def _validate_request_identity(self, request, intent_id, request_id, index):
        if (request.get('request_id') != request_id
                or not isinstance(request.get('intent_id'), str)
                or not request.get('intent_id')
                or not isinstance(request.get('action_id'), str)
                or not request.get('action_id')
                or not isinstance(request.get('trade_id'), str)
                or not request.get('trade_id')
                or request.get('operation') not in {'review', 'place', 'cancel'}):
            raise ValueError('HOST_DELIVERY_REQUEST_IDENTITY_MISMATCH')
        if request.get('intent_id') != intent_id:
            raise ValueError('HOST_DELIVERY_REQUEST_INTENT_MISMATCH')
        if request_id != self.request_id(intent_id,
                                         request.get('operation')):
            raise ValueError('HOST_DELIVERY_REQUEST_IDENTITY_MISMATCH')
        for key in ('intent_id', 'request_id', 'operation', 'action_id',
                    'trade_id'):
            if index.get(key) != request.get(key):
                raise ValueError('HOST_DELIVERY_INDEX_REQUEST_MISMATCH')
        return request

    def _validated_delivery(self, intent_id, request_id=None):
        index = self._read_index(intent_id)
        if index is None:
            raise ValueError('HOST_DELIVERY_INDEX_MISSING')
        if request_id is not None and index.get('request_id') != request_id:
            raise ValueError('HOST_DELIVERY_INDEX_REQUEST_MISMATCH')
        request = self._load_request(index['request_id'])
        self._validate_request_identity(request, intent_id,
                                        index['request_id'], index)
        return index, request

    def _write_index(self, intent_id, request_id, operation, action_id, trade_id):
        import json
        value = {'intent_id': intent_id, 'request_id': request_id,
                 'operation': operation, 'action_id': action_id,
                 'trade_id': trade_id}
        folder = self.directory / 'deliveries'
        folder.mkdir(parents=True, exist_ok=True)
        path = self._index_path(intent_id)
        temp = path.with_suffix('.tmp')
        temp.write_text(json.dumps(value, sort_keys=True))
        temp.replace(path)

    def deliver(self, intent):
        import json
        if not isinstance(intent, dict):
            raise ValueError('ADAPTER_ENTRY_INTENT_REQUIRED')
        intent_id = intent.get('intent_id')
        action_id = intent.get('action_id')
        if not isinstance(intent_id, str) or not intent_id:
            raise ValueError('ADAPTER_INTENT_ID_REQUIRED')
        if not isinstance(action_id, str) or not action_id:
            raise ValueError('ADAPTER_ACTION_ID_REQUIRED')
        trade_id = intent.get('trade_id')
        if not isinstance(trade_id, str) or not trade_id:
            raise ValueError('ADAPTER_TRADE_ID_REQUIRED')
        operation = intent.get('operation')
        if operation is None:
            # Backward-compatible default for old callers. The default live
            # supervisor always states the operation explicitly.
            operation = 'cancel' if intent.get('kind') == 'cancel' else 'review'
        if operation not in {'review', 'place', 'cancel'}:
            raise ValueError('ADAPTER_OPERATION_INVALID')
        clock = getattr(self.bridge, 'clock', None) or self.bridge.book.clock
        request_id = self.request_id(intent_id, operation)
        request = {'schema_version': 1, 'binding': dict(self.bridge.binding),
                   'request_id': request_id, 'requested_at': clock().isoformat(),
                   'operation': operation,
                   'action_id': action_id, 'trade_id': trade_id,
                   'intent_id': intent_id}
        prior_index = self._read_index(intent_id)
        if prior_index is not None:
            same_action = (prior_index.get('action_id') == action_id
                           and prior_index.get('trade_id') == trade_id)
            same_operation = prior_index.get('operation') == operation
            review_to_place = (prior_index.get('operation') == 'review'
                               and operation == 'place')
            if not same_action or (not same_operation and not review_to_place):
                prior_path = self._request_path(prior_index['request_id'])
                prior = (json.loads(prior_path.read_text())
                         if prior_path.exists() else prior_index)
                conflict = self.directory / 'delivery-conflicts' / (request_id + '.json')
                conflict.parent.mkdir(parents=True, exist_ok=True)
                conflict.write_text(json.dumps({'request_id': request_id,
                                                'intent_id': intent_id,
                                                'first': prior, 'second': request},
                                               sort_keys=True))
                raise ValueError('HOST_DELIVERY_IDENTITY_CONFLICT')
        folder = self.directory / 'requests'
        folder.mkdir(parents=True, exist_ok=True)
        path = self._request_path(request_id)
        if path.exists():
            prior = json.loads(path.read_text())
            same = all(prior.get(key) == request[key] for key in
                       ('request_id', 'action_id', 'trade_id', 'operation', 'binding'))
            if not same:
                # The same intent identity cannot secretly become another
                # action or trade. Leave an auditable conflict and refuse.
                conflict = self.directory / 'delivery-conflicts' / (request_id + '.json')
                conflict.parent.mkdir(parents=True, exist_ok=True)
                conflict.write_text(json.dumps({'request_id': request_id,
                                                'intent_id': intent_id,
                                                'first': prior, 'second': request},
                                               sort_keys=True))
                raise ValueError('HOST_DELIVERY_IDENTITY_CONFLICT')
            result = {'status': 'HOST_DELIVERY_RECORDED',
                      'request_id': request_id, 'duplicate': True,
                      'operation': operation}
        else:
            temp = path.with_suffix('.tmp')
            temp.write_text(json.dumps(request, sort_keys=True))
            temp.replace(path)
            result = {'status': 'HOST_DELIVERY_RECORDED',
                      'request_id': request_id, 'operation': operation}
        self._write_index(intent_id, request_id, operation, action_id, trade_id)
        return result

    VERIFIED_STATUSES = frozenset({'HOST_RECEIPT_IMPORTED'})

    def _packet_path(self, request_id):
        return self.directory / 'packets' / (request_id + '.json')

    def _validated_packet_envelope(self, request_id, request, expected_packet=None):
        """Return the packet only when the on-disk envelope matches the request."""
        import json
        from trading_contracts.values import digest
        path = self._packet_path(request_id)
        if not path.exists():
            raise ValueError('HOST_RECEIPT_PACKET_MISSING')
        try:
            envelope = json.loads(path.read_text())
        except (OSError, ValueError):
            raise ValueError('HOST_RECEIPT_PACKET_UNREADABLE') from None
        packet = envelope.get('packet') if isinstance(envelope, dict) else None
        if (not isinstance(envelope, dict)
                or envelope.get('schema_version') != 1
                or envelope.get('request_id') != request_id
                or envelope.get('binding') != request.get('binding')
                or not isinstance(packet, dict)
                or packet.get('action_id') != request.get('action_id')
                or packet.get('operation') != request.get('operation')
                or (expected_packet is not None
                    and packet != expected_packet)):
            raise ValueError('HOST_RECEIPT_PACKET_BINDING_MISMATCH')
        declared = envelope.get('packet_sha256')
        if declared is not None and declared != digest(packet):
            raise ValueError('HOST_RECEIPT_PACKET_HASH_MISMATCH')
        return packet

    @staticmethod
    def _required_packet_hash(value, expected):
        supplied = value.get('packet_sha256')
        if not isinstance(supplied, str) or not supplied:
            raise ValueError('HOST_RECEIPT_PACKET_HASH_MISSING')
        import re
        if not re.fullmatch('[0-9a-f]{64}', supplied):
            raise ValueError('HOST_RECEIPT_PACKET_HASH_INVALID')
        if supplied != expected:
            raise ValueError('HOST_RECEIPT_PACKET_HASH_MISMATCH')

    def validate_raw_receipt(self, pending, value):
        """Identity gate used by the bridge before importing a host receipt.

        In the default supervised-host chain the HostDeliveryPort is the
        delivery authority, so its strict identity must be checked before
        ``NativeOperations.record`` can write a Book report.
        """
        import json
        from trading_contracts.values import digest
        if not isinstance(pending, dict) or not isinstance(value, dict):
            raise ValueError('HOST_RECEIPT_UNREADABLE')
        request_id = pending.get('request_id')
        packet = pending.get('packet') or {}
        self._load_request(request_id)
        index = self._index_for_request(request_id)
        if (index is None or not isinstance(index.get('intent_id'), str)
                or not index.get('intent_id')):
            raise ValueError('HOST_RECEIPT_NOT_PORT_DELIVERED')
        try:
            index, request = self._validated_delivery(
                index['intent_id'], request_id=str(request_id))
        except ValueError as exc:
            if str(exc) == 'HOST_DELIVERY_INDEX_MISSING':
                raise ValueError('HOST_RECEIPT_NOT_PORT_DELIVERED') from None
            raise
        if value.get('schema_version') != 1:
            raise ValueError('HOST_RECEIPT_SCHEMA_INVALID')
        if value.get('request_id') != request_id:
            raise ValueError('HOST_RECEIPT_REQUEST_ID_MISMATCH')
        if value.get('action_id') != request.get('action_id'):
            raise ValueError('HOST_RECEIPT_ACTION_ID_MISMATCH')
        expected_trade = request.get('trade_id')
        if (not isinstance(expected_trade, str) or not expected_trade
                or not isinstance(value.get('trade_id'), str)
                or not value.get('trade_id')
                or value.get('trade_id') != expected_trade):
            raise ValueError('HOST_RECEIPT_TRADE_ID_MISMATCH')
        if value.get('binding') != request.get('binding'):
            raise ValueError('HOST_RECEIPT_BINDING_MISMATCH')
        packet = self._validated_packet_envelope(request_id, request,
                                                 expected_packet=packet)
        operation_id = packet.get('operation_id')
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError('HOST_RECEIPT_PACKET_OPERATION_MISSING')
        if value.get('operation_id') != operation_id:
            raise ValueError('HOST_RECEIPT_PACKET_OPERATION_MISMATCH')
        self._required_packet_hash(value, digest(packet))
        return True

    def receipt(self, intent_id):
        """Return a verified bridge receipt for this exact intent, or a refusal.

        A file with the right name is not evidence: the result must carry the
        schema, the matching request id, action and binding, and must reference
        the exact packet (operation and packet hash) that this delivery caused.
        Pending, expired, unknown, corrupt or mismatched results are returned as
        ``verified: False``; they never become a successful receipt.
        """
        import json
        from trading_contracts.values import digest
        if not isinstance(intent_id, str) or not intent_id:
            raise ValueError('ADAPTER_INTENT_ID_REQUIRED')
        try:
            index = self._read_index(intent_id)
        except ValueError as exc:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': str(exc)}
        if index is None:
            request_id = self.request_id(intent_id)
            result_path = self.directory / 'results' / (request_id + '.json')
            if not result_path.exists():
                return None
            if not self._request_path(request_id).exists():
                return {'status': 'HOST_RECEIPT_UNVERIFIED',
                        'verified': False,
                        'reason': 'HOST_DELIVERY_REQUEST_MISSING',
                        'request_id': request_id}
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_DELIVERY_INDEX_MISSING',
                    'request_id': request_id}
        try:
            index, request = self._validated_delivery(intent_id)
        except ValueError as exc:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': str(exc)}
        request_id = index['request_id']
        result_path = self.directory / 'results' / (request_id + '.json')
        if not result_path.exists():
            return None
        try:
            value = json.loads(result_path.read_text())
        except (OSError, ValueError):
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_UNREADABLE',
                    'request_id': request_id}
        if not isinstance(value, dict) or value.get('schema_version') != 1:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_SCHEMA_INVALID',
                    'request_id': request_id}
        if value.get('request_id') != request_id:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_REQUEST_ID_MISMATCH',
                    'request_id': request_id}
        if value.get('action_id') != request.get('action_id'):
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_ACTION_ID_MISMATCH',
                    'request_id': request_id}
        expected_trade = request.get('trade_id')
        if (not isinstance(expected_trade, str) or not expected_trade
                or not isinstance(value.get('trade_id'), str)
                or not value.get('trade_id')
                or value.get('trade_id') != expected_trade):
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_TRADE_ID_MISMATCH',
                    'request_id': request_id}
        if value.get('binding') != request.get('binding'):
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_BINDING_MISMATCH',
                    'request_id': request_id}
        try:
            packet = self._validated_packet_envelope(request_id, request)
        except ValueError as exc:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': str(exc), 'request_id': request_id}
        operation_id = packet.get('operation_id')
        if not isinstance(operation_id, str) or not operation_id:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_PACKET_OPERATION_MISSING',
                    'request_id': request_id}
        if value.get('operation_id') != operation_id:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': 'HOST_RECEIPT_PACKET_OPERATION_MISMATCH',
                    'request_id': request_id}
        try:
            self._required_packet_hash(value, digest(packet))
        except ValueError as exc:
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'reason': str(exc), 'request_id': request_id}
        status = value.get('status')
        if status not in self.VERIFIED_STATUSES:
            # Pending / expired / invalid / UNKNOWN bridge outcomes are not a
            # successful receipt. Keep the reason, claim nothing.
            return {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
                    'bridge_status': status,
                    'reason': 'HOST_RECEIPT_NOT_IMPORTED',
                    'request_id': request_id}
        value = dict(value)
        value['verified'] = True
        return value

