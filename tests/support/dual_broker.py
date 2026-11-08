"""Persistent two-symbol fake broker used only by 078r3b child tests."""
import json
import os
import uuid
from decimal import Decimal
from pathlib import Path

from test_e4_dual_host import MultiBroker


class PersistentBroker(MultiBroker):
    def save(self, path, *, barrier=None, checkpoint=None):
        path = Path(path)
        value = {
            'orders': self.orders,
            'positions': {key: str(value) for key, value in self.positions.items()},
            'cash': str(self.cash),
            'mark_prices': {key: str(value) for key, value in
                            self.mark_prices.items()},
            'unconfirmed_protections': sorted(self.unconfirmed_protections),
            'fill_queue': self.fill_queue,
            'ignore_cancel_orders': sorted(self.ignore_cancel_orders),
            'saved_at': self.clock().isoformat(),
        }
        if barrier is not None:
            value['barrier'] = barrier
        if checkpoint is not None:
            value['checkpoint'] = checkpoint
        tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        with tmp.open('w') as handle:
            json.dump(value, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o600)
        tmp.replace(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    @classmethod
    def load(cls, path, clock):
        path = Path(path)
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError('BROKER_FACTS_MISSING')
        try:
            value = json.loads(path.read_text())
        except (OSError, ValueError):
            raise ValueError('BROKER_FACTS_INVALID') from None
        if not isinstance(value, dict):
            raise ValueError('BROKER_FACTS_INVALID')
        required = ('orders', 'positions', 'cash', 'saved_at')
        missing = [key for key in required if key not in value]
        if missing:
            raise ValueError('BROKER_FACTS_INCOMPLETE:' + ','.join(missing))
        if (not isinstance(value['orders'], list)
                or not isinstance(value['positions'], dict)
                or not isinstance(value['cash'], (str, int, float))
                or not isinstance(value['saved_at'], str)):
            raise ValueError('BROKER_FACTS_INVALID')
        try:
            for order in value['orders']:
                if not isinstance(order, dict) or not isinstance(
                        order.get('executions', []), list):
                    raise ValueError
            for position in value['positions'].values():
                Decimal(str(position))
            Decimal(str(value['cash']))
            from datetime import datetime
            datetime.fromisoformat(value['saved_at'])
        except (TypeError, ValueError):
            raise ValueError('BROKER_FACTS_INVALID') from None
        saved = value.get('saved_at')
        if saved:
            from datetime import datetime
            clock.start = datetime.fromisoformat(saved)
            clock.monotonic = __import__('time').monotonic()
            clock.offset = 0.0
        state = cls(clock)
        state.orders = value.get('orders', [])
        state.positions = {key: Decimal(str(v)) for key, v in
                           (value.get('positions') or {}).items()}
        state.cash = Decimal(str(value.get('cash', '500')))
        state.mark_prices = {
            key: Decimal(str(price)) for key, price
            in (value.get('mark_prices') or {}).items()}
        state.unconfirmed_protections = set(
            value.get('unconfirmed_protections') or [])
        state.fill_queue = {
            key: [str(item) for item in queue]
            for key, queue in (value.get('fill_queue') or {}).items()}
        state.ignore_cancel_orders = set(
            value.get('ignore_cancel_orders') or [])
        return state, value
