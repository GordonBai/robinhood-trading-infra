import json
from pathlib import Path
import tempfile
from unittest import TestCase
from martin_agentic.audit import AuditTrail
from martin_agentic.audit_anchor import export_anchor,verify_anchor


class AuditAnchorTests(TestCase):
    def test_separately_retained_prefix_survives_append_and_detects_different_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'audit.sqlite';audit=AuditTrail(db)
            event={'event_id':'one','occurred_at':'2026-10-19T20:00:00+00:00','event_type':'ACCOUNT_OBSERVED',
                'actor':'broker','synthetic':False,'account':'••••0000','details':{'fixture':True}}
            audit.append(event);output=Path(directory)/'anchor.json';anchor=export_anchor(db,output)
            audit.append({**event,'event_id':'two'})
            self.assertTrue(verify_anchor(db,anchor)['prefix_matches'])
            self.assertFalse(anchor['external_immutable_storage_verified'])
            with self.assertRaises(FileExistsError):export_anchor(db,output)
            other=Path(directory)/'other.sqlite';second=AuditTrail(other)
            second.append({**event,'event_id':'different-stream'})
            with self.assertRaisesRegex(ValueError,'DIFFERS'):verify_anchor(other,json.loads(output.read_text()))
            audit.close();second.close()
