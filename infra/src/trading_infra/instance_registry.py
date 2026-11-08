"""Durable strategy-instance registry and per-instance state namespaces.

One account runs many strategy instances. This module owns the durable facts
that make them separable: which plugin version and configuration each instance
registered, its own budget ceiling, its own mode, and its own checkpoint row.
It never places orders and never decides entries.

The registry lives on the execution owner's connection so that registration,
state commits and execution bookkeeping share one transaction domain.
"""
from datetime import datetime, timezone
import json

from trading_contracts.instance import (LEGACY_INSTANCE_ID, MODE_SCHEMA_VERSION,
                                        MODES, InstanceBudget, PluginManifest,
                                        StrategyInstance)
from trading_contracts.values import canonical, digest

LEGACY_PLUGIN_ID = 'martin'

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS strategy_instances(
    instance_id TEXT PRIMARY KEY,
    plugin_id TEXT NOT NULL, version TEXT NOT NULL, state_schema TEXT NOT NULL,
    config_json TEXT NOT NULL, config_fingerprint TEXT NOT NULL, manifest_digest TEXT NOT NULL,
    plugin_identity TEXT,
    data_requirements TEXT NOT NULL, capabilities TEXT NOT NULL, max_inflight INTEGER NOT NULL,
    max_cash TEXT NOT NULL, max_planned_loss TEXT NOT NULL, max_notional TEXT NOT NULL,
    max_concurrent INTEGER NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('shadow','read_only_shadow','disabled')),
    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS strategy_instance_state(
    instance_id TEXT NOT NULL REFERENCES strategy_instances(instance_id),
    state_key TEXT NOT NULL, payload TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY(instance_id,state_key));
CREATE TABLE IF NOT EXISTS strategy_instance_schema(
    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS strategy_instance_events(
    instance_id TEXT NOT NULL, sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    record_kind TEXT NOT NULL, record_id TEXT NOT NULL, detail TEXT NOT NULL,
    recorded_at TEXT NOT NULL, UNIQUE(instance_id,record_kind,record_id));
'''

_COLUMNS = ('instance_id', 'plugin_id', 'version', 'state_schema', 'config_json',
            'config_fingerprint', 'manifest_digest', 'plugin_identity',
            'data_requirements', 'capabilities',
            'max_inflight', 'max_cash', 'max_planned_loss', 'max_notional', 'max_concurrent',
            'mode', 'enabled', 'created_at', 'updated_at')
_LEGACY_COLUMNS = tuple(name for name in _COLUMNS if name != 'plugin_identity')


def _instance_id(value):
    from trading_contracts.instance import _label
    return _label(value, 'ID')


class InstanceRegistry:
    """Register, read and disable strategy instances. No execution authority."""

    def __init__(self, db, *, clock=None):
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        if self.db.in_transaction:
            raise ValueError('INSTANCE_SCHEMA_REQUIRES_IDLE_CONNECTION')
        self._check_schema_before_write()
        self.db.executescript(_SCHEMA)
        self._ensure_mode_schema()

    def _schema_version(self):
        table = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table'"
            " AND name='strategy_instance_schema'").fetchone()
        if not table:
            return None
        row = self.db.execute(
            'SELECT version FROM strategy_instance_schema WHERE id=1').fetchone()
        if row is None:
            return None
        if type(row[0]) is not int or row[0] < 1:
            raise ValueError('INSTANCE_SCHEMA_VERSION_INVALID')
        return row[0]

    def _structure_shape(self):
        import re
        row = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table'"
            " AND name='strategy_instances'").fetchone()
        if row is None:
            return None
        columns = self.db.execute(
            'PRAGMA table_info(strategy_instances)').fetchall()
        names = [item[1] for item in columns]
        primary = [item[1] for item in columns if item[5]]
        if names not in (list(_COLUMNS), list(_LEGACY_COLUMNS)) or primary != ['instance_id']:
            return None
        sql = re.sub(r'\s+', '', (row[0] or '').lower())
        if ('check(modein(\'shadow\',\'read_only_shadow\',\'disabled\'))'
                in sql and 'check(enabledin(0,1))' in sql):
            return 'v2'
        if ('check(modein(\'shadow\',\'disabled\'))' in sql
                and 'check(enabledin(0,1))' in sql):
            return 'v1'
        return None

    def _check_schema_before_write(self):
        version = self._schema_version()
        if version is not None and version > MODE_SCHEMA_VERSION:
            raise ValueError('INSTANCE_SCHEMA_VERSION_UNSUPPORTED')
        if version == MODE_SCHEMA_VERSION and self._structure_shape() != 'v2':
            # A current-version marker cannot authorize an older shape.
            raise ValueError('INSTANCE_SCHEMA_STRUCTURE_MISMATCH')

    def _ensure_mode_schema(self):
        """Migrate old schemas without silently rewriting a future version."""
        version = self._schema_version()
        if version is not None and version > MODE_SCHEMA_VERSION:
            raise ValueError('INSTANCE_SCHEMA_VERSION_UNSUPPORTED')
        shape = self._structure_shape()
        if version == MODE_SCHEMA_VERSION:
            if shape != 'v2':
                raise ValueError('INSTANCE_SCHEMA_STRUCTURE_MISMATCH')
            columns = {row[1] for row in self.db.execute(
                'PRAGMA table_info(strategy_instances)')}
            if 'plugin_identity' not in columns:
                self.db.execute('ALTER TABLE strategy_instances'
                                ' ADD COLUMN plugin_identity TEXT')
            return
        if version not in (None, 1):
            raise ValueError('INSTANCE_SCHEMA_VERSION_INVALID')
        if shape == 'v2':
            if version is None:
                self._write_version()
                return
            raise ValueError('INSTANCE_SCHEMA_STRUCTURE_MISMATCH')
        if shape != 'v1':
            raise ValueError('INSTANCE_SCHEMA_STRUCTURE_MISMATCH')
        if self.db.in_transaction:
            raise ValueError('INSTANCE_SCHEMA_MIGRATION_REQUIRES_IDLE_CONNECTION')
        columns = ','.join(_COLUMNS)
        source_columns = ','.join(
            'NULL' if name == 'plugin_identity' else name for name in _COLUMNS)
        previous_fk = self.db.execute('PRAGMA foreign_keys').fetchone()[0]
        try:
            if previous_fk:
                self.db.execute('PRAGMA foreign_keys=OFF')
            self.db.execute('BEGIN IMMEDIATE')
            self.db.execute("""CREATE TABLE strategy_instances_v2(
                instance_id TEXT PRIMARY KEY,
                plugin_id TEXT NOT NULL, version TEXT NOT NULL, state_schema TEXT NOT NULL,
                config_json TEXT NOT NULL, config_fingerprint TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                plugin_identity TEXT,
                data_requirements TEXT NOT NULL, capabilities TEXT NOT NULL, max_inflight INTEGER NOT NULL,
                max_cash TEXT NOT NULL, max_planned_loss TEXT NOT NULL, max_notional TEXT NOT NULL,
                max_concurrent INTEGER NOT NULL,
                mode TEXT NOT NULL CHECK(mode IN ('shadow','read_only_shadow','disabled')),
                enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
            self.db.execute(f'INSERT INTO strategy_instances_v2({columns})'
                            f' SELECT {source_columns} FROM strategy_instances')
            self.db.execute('DROP TABLE strategy_instances')
            self.db.execute('ALTER TABLE strategy_instances_v2'
                            ' RENAME TO strategy_instances')
            self.db.execute('INSERT INTO strategy_instance_schema(id,version)'
                            ' VALUES(1,?) ON CONFLICT(id) DO UPDATE SET version=?',
                            (MODE_SCHEMA_VERSION, MODE_SCHEMA_VERSION))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        finally:
            if previous_fk:
                self.db.execute('PRAGMA foreign_keys=ON')

    def _write_version(self):
        if self.db.in_transaction:
            raise ValueError('INSTANCE_SCHEMA_MIGRATION_REQUIRES_IDLE_CONNECTION')
        self.db.execute('INSERT INTO strategy_instance_schema(id,version)'
                        ' VALUES(1,?) ON CONFLICT(id) DO UPDATE SET version=?',
                        (MODE_SCHEMA_VERSION, MODE_SCHEMA_VERSION))
        self.db.commit()

    def _now(self):
        return self.clock().astimezone(timezone.utc).isoformat()

    def _record(self, row):
        values = dict(zip(_COLUMNS, row))
        manifest = PluginManifest(
            plugin_id=values['plugin_id'], version=values['version'],
            state_schema=values['state_schema'],
            config=json.loads(values['config_json']),
            data_requirements=tuple(json.loads(values['data_requirements'])),
            capabilities=tuple(json.loads(values['capabilities'])),
            max_inflight=values['max_inflight'],
            plugin_identity=values.get('plugin_identity'))
        budget = InstanceBudget(max_cash=values['max_cash'],
                                max_planned_loss=values['max_planned_loss'],
                                max_notional=values['max_notional'],
                                max_concurrent=values['max_concurrent'])
        return StrategyInstance(instance_id=values['instance_id'], manifest=manifest,
                                budget=budget, mode=values['mode'],
                                enabled=bool(values['enabled']))

    def register(self, instance_id, manifest, budget, *, mode='shadow', enabled=False):
        """Idempotent registration. A changed manifest or budget is refused.

        Budget and manifest changes are not silent updates: an already-running
        instance keeps the ceiling it was admitted with. Changing either one is
        a new registration under a new instance id, so no state can inherit a
        wider allowance than the one it was reviewed under.
        """
        instance_id = _instance_id(instance_id)
        if not isinstance(manifest, PluginManifest):
            raise ValueError('INSTANCE_MANIFEST_REQUIRED')
        if not isinstance(budget, InstanceBudget):
            raise ValueError('INSTANCE_BUDGET_REQUIRED')
        if mode not in MODES or type(enabled) is not bool:
            raise ValueError('INSTANCE_MODE_INVALID')
        if mode == 'disabled' and enabled:
            raise ValueError('INSTANCE_DISABLED_MODE_CANNOT_BE_ENABLED')
        now = self._now()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            prior = self.db.execute('SELECT ' + ','.join(_COLUMNS) +
                                    ' FROM strategy_instances WHERE instance_id=?',
                                    (instance_id,)).fetchone()
            payload = (manifest.plugin_id, manifest.version, manifest.state_schema,
                       canonical(dict(manifest.config)), manifest.config_fingerprint(),
                       manifest.digest(), manifest.plugin_identity,
                       canonical(list(manifest.data_requirements)),
                       canonical(list(manifest.capabilities)), manifest.max_inflight,
                       str(budget.max_cash), str(budget.max_planned_loss),
                       str(budget.max_notional), budget.max_concurrent)
            if prior is None:
                self.db.execute('INSERT INTO strategy_instances(' + ','.join(_COLUMNS) + ')'
                                ' VALUES(' + ','.join('?' * len(_COLUMNS)) + ')',
                                (instance_id,) + payload + (mode, int(enabled), now, now))
            else:
                existing = self._record(prior)
                same_contract = (existing.manifest.digest() == manifest.digest()
                                 and existing.budget.as_dict() == budget.as_dict())
                if not same_contract:
                    raise ValueError('INSTANCE_REGISTRATION_CONFLICT')
                if existing.mode != mode or existing.enabled != enabled:
                    raise ValueError('INSTANCE_MODE_CHANGE_REQUIRES_SET_MODE')
            self.db.commit()
            return self.instance(instance_id)
        except BaseException:
            self.db.rollback()
            raise

    def upgrade_data_requirements(self, instance_id, manifest, budget):
        """Explicitly add declared inputs without changing any other contract.

        A changed manifest is normally a conflict, because state admitted under
        one contract must not silently inherit another. This migration is the
        narrow exception: same plugin/version/schema/config/capabilities/limits
        and budget, with the old input set a proper subset of the new one. It
        updates only the routing declaration, preserves checkpoint state, and
        leaves an instance-scoped audit event.
        """
        instance_id = _instance_id(instance_id)
        if not isinstance(manifest, PluginManifest) or not isinstance(budget, InstanceBudget):
            raise ValueError('INSTANCE_MANIFEST_OR_BUDGET_REQUIRED')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute('SELECT ' + ','.join(_COLUMNS) +
                                  ' FROM strategy_instances WHERE instance_id=?',
                                  (instance_id,)).fetchone()
            if row is None:
                raise ValueError('INSTANCE_NOT_REGISTERED')
            existing = self._record(row)
            old = existing.manifest
            old_requirements = tuple(old.data_requirements)
            new_requirements = tuple(manifest.data_requirements)
            same_except_inputs = (
                old.plugin_id == manifest.plugin_id
                and old.version == manifest.version
                and old.state_schema == manifest.state_schema
                and old.config == manifest.config
                and old.config_fingerprint() == manifest.config_fingerprint()
                and old.capabilities == manifest.capabilities
                and old.max_inflight == manifest.max_inflight
                and existing.budget.as_dict() == budget.as_dict())
            if (not same_except_inputs
                    or old_requirements == new_requirements
                    or not set(old_requirements) < set(new_requirements)):
                raise ValueError('INSTANCE_REGISTRATION_CONFLICT')
            now = self._now()
            self.db.execute(
                'UPDATE strategy_instances SET data_requirements=?,'
                ' manifest_digest=?, updated_at=? WHERE instance_id=?',
                (canonical(list(new_requirements)), manifest.digest(), now,
                 instance_id))
            self.db.execute(
                'INSERT OR IGNORE INTO strategy_instance_events'
                '(instance_id,record_kind,record_id,detail,recorded_at)'
                ' VALUES(?,?,?,?,?)',
                (instance_id, 'manifest_upgrade', old.digest(),
                 canonical({'status': 'DATA_REQUIREMENTS_ADDED',
                            'old_manifest_digest': old.digest(),
                            'new_manifest_digest': manifest.digest(),
                            'old_data_requirements': old_requirements,
                            'new_data_requirements': new_requirements,
                            'checkpoint_preserved': True}), now))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.instance(instance_id)

    def instance(self, instance_id):
        row = self.db.execute('SELECT ' + ','.join(_COLUMNS) +
                              ' FROM strategy_instances WHERE instance_id=?',
                              (_instance_id(instance_id),)).fetchone()
        if row is None:
            raise ValueError('INSTANCE_NOT_REGISTERED')
        return self._record(row)

    def find(self, instance_id):
        row = self.db.execute('SELECT ' + ','.join(_COLUMNS) +
                              ' FROM strategy_instances WHERE instance_id=?',
                              (_instance_id(instance_id),)).fetchone()
        return self._record(row) if row else None

    def instances(self):
        rows = self.db.execute('SELECT ' + ','.join(_COLUMNS) +
                               ' FROM strategy_instances ORDER BY instance_id').fetchall()
        return [self._record(row) for row in rows]

    def enabled_instances(self):
        return [item for item in self.instances() if item.enabled and item.mode != 'disabled']

    def set_mode(self, instance_id, mode, *, enabled):
        """Change availability only. Never changes a registered contract."""
        if mode not in MODES or type(enabled) is not bool:
            raise ValueError('INSTANCE_MODE_INVALID')
        if mode == 'disabled' and enabled:
            raise ValueError('INSTANCE_DISABLED_MODE_CANNOT_BE_ENABLED')
        self.instance(instance_id)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('UPDATE strategy_instances SET mode=?,enabled=?,updated_at=?'
                            ' WHERE instance_id=?',
                            (mode, int(enabled), self._now(), instance_id))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.instance(instance_id)

    def record(self, instance_id, record_kind, record_id, detail):
        """Append one instance-scoped diagnostic. Repeats are ignored, not merged."""
        self.db.execute('INSERT OR IGNORE INTO strategy_instance_events'
                        '(instance_id,record_kind,record_id,detail,recorded_at)'
                        ' VALUES(?,?,?,?,?)',
                        (instance_id, record_kind, record_id, canonical(detail), self._now()))

    def events(self, instance_id):
        return [dict(instance_id=row[0], sequence=row[1], record_kind=row[2],
                     record_id=row[3], detail=json.loads(row[4]), recorded_at=row[5])
                for row in self.db.execute(
                    'SELECT instance_id,sequence,record_kind,record_id,detail,recorded_at'
                    ' FROM strategy_instance_events WHERE instance_id=? ORDER BY sequence',
                    (instance_id,))]

    # -- per-instance strategy state -------------------------------------
    def state(self, instance_id, state_key='checkpoint'):
        row = self.db.execute('SELECT payload FROM strategy_instance_state'
                              ' WHERE instance_id=? AND state_key=?',
                              (instance_id, state_key)).fetchone()
        return json.loads(row[0]) if row else None

    def put_state(self, instance_id, payload, *, state_key='checkpoint', expected=None):
        """Compare-and-set the instance checkpoint.

        ``expected`` is the canonical text previously read, or None for "no row
        yet". A mismatch means another writer advanced this instance, and the
        caller must recompute rather than overwrite newer state.
        """
        if not isinstance(payload, dict):
            raise ValueError('INSTANCE_STATE_PAYLOAD_REQUIRED')
        body = canonical(payload)
        prior = self.db.execute('SELECT payload FROM strategy_instance_state'
                                ' WHERE instance_id=? AND state_key=?',
                                (instance_id, state_key)).fetchone()
        if expected is not None:
            if prior is None or prior[0] != canonical(expected):
                raise ValueError('INSTANCE_STATE_CONFLICT')
        elif prior is not None:
            raise ValueError('INSTANCE_STATE_CONFLICT')
        self.db.execute('INSERT INTO strategy_instance_state'
                        '(instance_id,state_key,payload,updated_at) VALUES(?,?,?,?)'
                        ' ON CONFLICT(instance_id,state_key)'
                        ' DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at',
                        (instance_id, state_key, body, self._now()))
        return digest(['strategy-instance-state-1', instance_id, state_key, body])

    def state_digest(self, instance_id, state_key='checkpoint'):
        value = self.state(instance_id, state_key)
        return None if value is None else digest(['strategy-instance-state-1',
                                                  instance_id, state_key, canonical(value)])


def migrate_legacy_live_strategy(db, registry, *, instance_id=LEGACY_INSTANCE_ID):
    """Copy the single legacy ``live_strategy`` row into one instance namespace.

    The legacy table is read-only afterwards: this never writes it, never
    deletes it, and refuses to overwrite an instance whose state already
    advanced. Re-running is a no-op once the copy exists and matches.
    """
    present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table'"
                         " AND name='live_strategy'").fetchone()
    if not present:
        return {'migrated': False, 'reason': 'LEGACY_TABLE_ABSENT'}
    row = db.execute('SELECT payload FROM live_strategy WHERE id=1').fetchone()
    if row is None:
        return {'migrated': False, 'reason': 'LEGACY_STATE_ABSENT'}
    payload = json.loads(row[0])
    registry.instance(instance_id)
    existing = registry.state(instance_id)
    if existing is None:
        registry.put_state(instance_id, payload, expected=None)
        return {'migrated': True, 'reason': 'COPIED', 'instance_id': instance_id}
    if canonical(existing) == canonical(payload):
        return {'migrated': False, 'reason': 'ALREADY_CURRENT', 'instance_id': instance_id}
    raise ValueError('INSTANCE_STATE_CONFLICT')
