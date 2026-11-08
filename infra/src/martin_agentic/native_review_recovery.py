"""Explicit import of a persisted review after a process crash; never broker I/O."""
import json
import os
from pathlib import Path
import re
import stat

from .audit import digest
from .control_tasks import worker_lease
from .ledger import timestamp
from .native_operations import NativeOperations


def recover_native_review(book,operation_id,receipt_dir):
    """Recover metadata only. Original review age and confirmation survive import.

    Hashes detect accidental mismatch, not malicious local-owner modification.
    No source build match is required: recovery may follow a software upgrade.
    No action reservation, order replay, or broker observation is performed.
    """
    if not isinstance(operation_id,str) or not re.fullmatch(r'native-[0-9a-f-]{36}',operation_id):
        raise ValueError('REVIEW_RECOVERY_OPERATION_ID_INVALID')
    suffix = str(book.policy.get('account_suffix', ''))
    if not (suffix.isdigit() and len(suffix) == 4) or book.policy.get('capture_only') is True:
        raise ValueError('REVIEW_RECOVERY_POLICY_SCOPE_DENIED')
    db=book.db.execute('PRAGMA database_list').fetchone()[2]
    if not db:raise ValueError('FILE_BACKED_WORKER_LEASE_REQUIRED')
    lease=worker_lease(db)
    if lease is None:raise ValueError('WORKER_OWNER_STILL_ACTIVE')
    try:
        if not book.audit.verify()['integrity_valid']:raise ValueError('REVIEW_RECOVERY_AUDIT_INVALID')
        if not book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_operation_records'").fetchone():
            raise ValueError('REVIEW_RECOVERY_OPERATION_NOT_FOUND')
        row=book.db.execute('SELECT operation,payload,created_at FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
        if not row:raise ValueError('REVIEW_RECOVERY_OPERATION_NOT_FOUND')
        packet=json.loads(row[1])
        if (row[0]!='review' or packet.get('operation')!='review'
                or packet.get('tool')!='review_equity_order' or packet.get('account')!='••••'+suffix
                or packet.get('operation_id')!=operation_id):
            raise ValueError('REVIEW_RECOVERY_REVIEW_ONLY')
        state=book.state()
        if packet.get('account_key')!=state.get('account_key') or not state.get('account_key'):
            raise ValueError('REVIEW_RECOVERY_ACCOUNT_MISMATCH')
        path=Path(receipt_dir).resolve()/(operation_id+'.json')
        descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(descriptor,'rb') as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):raise ValueError('REVIEW_RECOVERY_REGULAR_FILE_REQUIRED')
            raw=handle.read(2_000_001)
        if len(raw)>2_000_000:raise ValueError('REVIEW_RECOVERY_RECEIPT_TOO_LARGE')
        receipt=json.loads(raw)
        expected={'db':str(Path(db).resolve()),'account_key':state['account_key'],
            'policy_sha256':digest(book.policy),'synthetic':book.simulation}
        binding=receipt['binding']
        if not isinstance(binding,dict) or set(binding) not in (set(expected),set(expected)|{'source_fingerprint'}):
            raise ValueError('REVIEW_RECOVERY_BINDING_INVALID')
        if (type(binding['synthetic']) is not bool or any(binding[key]!=value for key,value in expected.items())
                or 'source_fingerprint' in binding and (not isinstance(binding['source_fingerprint'],str)
                    or not re.fullmatch('[0-9a-f]{64}',binding['source_fingerprint']))):
            raise ValueError('REVIEW_RECOVERY_BINDING_INVALID')
        if (receipt['packet']!=packet or receipt['packet_sha256']!=digest(packet)
                or receipt['response_sha256']!=digest(receipt['response'])
                or receipt.get('broker_effect_verified') is not False):
            raise ValueError('REVIEW_RECOVERY_RECEIPT_INVALID')
        if not timestamp(row[2])<=timestamp(receipt['received_at'])<=book.clock():
            raise ValueError('REVIEW_RECOVERY_RECEIPT_TIME_INVALID')
        result=NativeOperations(book).record(operation_id,receipt['response'])
        action=next(a for a in book.actions() if a['id']==packet['action_id'])
        return {'status':'REVIEW_METADATA_RECOVERED','operation_id':operation_id,
            'action_id':packet['action_id'],'action_state':action['state'],'receipt_file':str(path),
            'original_review_created_at':row[2],'native_result':result,
            'broker_calls':0,'model_calls':0,'execution_authorized':False,
            'broker_effect_verified':False,'automatic_replay_allowed':False,
            'requires_fresh_bound_broker_reconciliation':True,'production_ready':False}
    finally:lease.close()
