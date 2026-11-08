"""Portable audit-prefix commitments. External storage trust is not assumed."""
from contextlib import closing
from datetime import datetime,timezone
import json
from pathlib import Path
import re
import sqlite3

from .audit import AuditTrail,canonical


def _view(connection):
    audit=object.__new__(AuditTrail);audit.db=connection
    return audit


def export_anchor(db_path,output_path,*,clock=None):
    db=Path(db_path).resolve();target=Path(output_path)
    if not db.is_file():raise ValueError('ANCHOR_DATABASE_MISSING')
    with closing(sqlite3.connect(db.as_uri()+'?mode=ro',uri=True,timeout=2)) as connection:
        connection.execute('BEGIN');verified=_view(connection).verify()
        row=connection.execute('SELECT sequence,entry_hash FROM audit_events ORDER BY sequence DESC LIMIT 1').fetchone()
        if not row:raise ValueError('ANCHOR_NONEMPTY_AUDIT_REQUIRED')
        anchor={'schema_version':1,'generated_at':(clock or (lambda:datetime.now(timezone.utc)))().isoformat(),
            'head_sequence':row[0],'head_hash':row[1],'events_committed':verified['events_verified'],
            'scope':'local_audit_prefix_commitment','external_immutable_storage_verified':False,
            'broker_receipts_cryptographically_authenticated':False}
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('x',encoding='utf-8') as output:output.write(canonical(anchor)+'\n')
    return anchor|{'path':str(target.resolve()),'overwrites_existing_anchor':False}


def verify_anchor(db_path,anchor):
    if (anchor.get('schema_version')!=1 or type(anchor.get('head_sequence')) is not int
            or anchor['head_sequence']<=0 or type(anchor.get('events_committed')) is not int
            or not isinstance(anchor.get('head_hash'),str) or not re.fullmatch('[0-9a-f]{64}',anchor['head_hash'])):
        raise ValueError('ANCHOR_SCHEMA_INVALID')
    with closing(sqlite3.connect(Path(db_path).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as connection:
        connection.execute('BEGIN');_view(connection).verify()
        row=connection.execute('SELECT entry_hash FROM audit_events WHERE sequence=?',(anchor['head_sequence'],)).fetchone()
        count=connection.execute('SELECT count(*) FROM audit_events WHERE sequence<=?',(anchor['head_sequence'],)).fetchone()[0]
        if not row or row[0]!=anchor['head_hash'] or count!=anchor['events_committed']:
            raise ValueError('AUDIT_PREFIX_DIFFERS_FROM_ANCHOR')
    return {'prefix_matches':True,'head_sequence':anchor['head_sequence'],
        'anchor_trust_must_be_established_independently':True,'external_immutable_storage_verified':False,
        'broker_receipts_cryptographically_authenticated':False}
