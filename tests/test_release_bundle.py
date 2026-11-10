from contextlib import redirect_stdout,redirect_stderr
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import canonical,digest
from martin_agentic.qualification import source_fingerprint
from martin_agentic.release_bundle import build_release_bundle,verify_release_bundle


class ReleaseBundleTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'source';self.root.mkdir()
        for name,body in {'infra/src/martin_agentic/demo.py':'value=1\n','tests/test_demo.py':'# test\n',
                'pyproject.toml':'[project]\nname="test"\n','uv.lock':'version=1\n',
                'examples/input.json':'{"synthetic":true}',
                'reports/community_tool_contract_evidence.json':'{"tools":[]}',
                'runtime/do-not-package.sqlite':'private','reports/private.json':'private',
                '.env':'secret','README.md':'Test bundle\n','README.zh-CN.md':'Test bundle zh\n',
                'LICENSE':'MIT License\n','CONTRIBUTING.md':'Contributing\n',
                '.github/PULL_REQUEST_TEMPLATE.md':'PR template\n',
                '.github/private-extra.md':'not packaged\n','notes.txt':'not packaged\n',
                'tests/fixtures/legacy-artifact-579e/manifest.json':'{"source_fingerprint":"'+'a'*64+'"}\n',
                'tests/fixtures/robinhood_historicals_before_first_bar.json':'{"data":[],"error":"synthetic"}\n',
                'tests/fixtures/formal-tool-contract.json':'{"synthetic":true,"historical":true,"tools":[]}\n',
                'tests/fixtures/synthetic_recording.json':'{"synthetic":true}\n',
                'tests/fixtures/probe_plugins/pyproject.toml':'[project]\nname="probe"\n',
                'tests/fixtures/policies/live_trial_2026-10-16.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/live_trial_2026-10-19.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/live_trial_2026-10-21.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/live_trial_2026-10-22.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/live_trial_2026-10-26.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/read_only_capture_2026-10-20.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/read_only_capture_2026-10-21.json':'{"synthetic":true}\n',
                'tests/fixtures/policies/read_only_capture_2026-10-27.json':'{"synthetic":true}\n'}.items():
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(body)
        self.output=Path(self.tmp.name)/'bundle'

    def build(self):return build_release_bundle(self.root,self.output)

    def manifest(self):return json.loads((self.output/'manifest.json').read_text())

    def save_manifest(self,m):
        m['input_set_sha256']=digest(m['files']);m['file_count']=len(m['files'])
        (self.output/'manifest.json').write_text(canonical(m))

    def pins(self):
        m=self.manifest()
        return {'expected_source':m['source_fingerprint'],'expected_input_set_sha256':m['input_set_sha256']}

    def test_deterministic_bundle_includes_fixtures_and_excludes_runtime_secrets(self):
        result=self.build();other=Path(self.tmp.name)/'other';again=build_release_bundle(self.root,other)
        self.assertEqual(result['archive_sha256'],again['archive_sha256'])
        names={r['path'] for r in self.manifest()['files']}
        self.assertIn('examples/input.json',names);self.assertIn('reports/community_tool_contract_evidence.json',names)
        self.assertIn('tests/fixtures/legacy-artifact-579e/manifest.json',names)
        self.assertIn('tests/fixtures/robinhood_historicals_before_first_bar.json',names)
        self.assertIn('tests/fixtures/formal-tool-contract.json',names)
        self.assertIn('LICENSE',names);self.assertIn('README.zh-CN.md',names)
        self.assertIn('CONTRIBUTING.md',names)
        self.assertIn('.github/PULL_REQUEST_TEMPLATE.md',names)
        self.assertNotIn('.github/private-extra.md',names);self.assertNotIn('notes.txt',names)
        self.assertNotIn('runtime/do-not-package.sqlite',names);self.assertNotIn('.env',names)
        self.assertNotIn('reports/private.json',names)
        extracted=Path(self.tmp.name)/'extracted'
        verified=verify_release_bundle(self.output,extract_to=extracted,**self.pins())
        self.assertEqual(source_fingerprint(extracted),source_fingerprint(self.root))
        self.assertEqual((extracted/'examples/input.json').read_text(),'{"synthetic":true}')
        self.assertFalse(verified['tests_executed']);self.assertFalse(verified['production_ready'])

    def test_public_root_allowlist_is_exact_not_whole_root_or_github(self):
        from martin_agentic.release_bundle import PUBLIC_ROOT_FILES
        self.assertEqual(set(PUBLIC_ROOT_FILES), {
            'README.md', 'README.zh-CN.md', 'LICENSE', 'CONTRIBUTING.md',
            '.github/PULL_REQUEST_TEMPLATE.md'})
        self.build()
        names={r['path'] for r in self.manifest()['files']}
        self.assertTrue(set(PUBLIC_ROOT_FILES) <= names)
        self.assertNotIn('.github/private-extra.md',names)
        self.assertNotIn('notes.txt',names)
        self.assertNotIn('.env',names)

    def test_fixture_change_changes_bundle_identity_even_when_qualified_source_is_unchanged(self):
        first=self.build();build=source_fingerprint(self.root)
        (self.root/'examples/input.json').write_text('{"synthetic":true,"changed":true}')
        second=build_release_bundle(self.root,Path(self.tmp.name)/'changed')
        self.assertEqual(build,source_fingerprint(self.root))
        self.assertNotEqual(first['input_set_sha256'],second['input_set_sha256'])
        self.assertNotEqual(first['archive_sha256'],second['archive_sha256'])

    def test_refuses_symlinked_source_or_fixture(self):
        path=self.root/'examples/link.json';path.symlink_to(self.root/'.env')
        with self.assertRaisesRegex(ValueError,'SYMLINK'):self.build()
        self.assertFalse(self.output.exists())

    def test_fixture_inventory_matches_discovered_non_python_fixtures(self):
        from martin_agentic.release_bundle import REQUIRED_TEST_INPUTS,_fixture_inventory
        self.assertEqual(_fixture_inventory(self.root),set(REQUIRED_TEST_INPUTS))

    def test_new_required_fixture_missing_refuses_build(self):
        from martin_agentic.release_bundle import REQUIRED_TEST_INPUTS
        missing='tests/fixtures/new-required-fixture.json'
        with patch('martin_agentic.release_bundle.REQUIRED_TEST_INPUTS',
                   REQUIRED_TEST_INPUTS|{missing}):
            with self.assertRaisesRegex(ValueError,
                    'RELEASE_REQUIRED_INPUT_MISSING:'+missing):self.build()
        self.assertFalse(self.output.exists())

    def test_required_fixture_change_changes_bundle_identity_not_source_hash(self):
        first=self.build();source=source_fingerprint(self.root)
        fixture=self.root/'tests/fixtures/robinhood_historicals_before_first_bar.json'
        fixture.write_text('{"data":[],"error":"synthetic-changed"}\n')
        second=build_release_bundle(self.root,Path(self.tmp.name)/'changed-fixture')
        self.assertEqual(source,source_fingerprint(self.root))
        self.assertNotEqual(first['input_set_sha256'],second['input_set_sha256'])
        self.assertNotEqual(first['archive_sha256'],second['archive_sha256'])

    def test_missing_contract_fixture_cannot_produce_release(self):
        (self.root/'reports/community_tool_contract_evidence.json').unlink()
        with self.assertRaisesRegex(ValueError,'REQUIRED_INPUT_MISSING'):self.build()

    def test_source_change_during_snapshot_never_publishes_release(self):
        from martin_agentic.release_bundle import _files
        calls=[]
        def changing(root):
            result=_files(root);calls.append(1)
            if len(calls)==1:(root/'infra/src/martin_agentic/demo.py').write_text('value=2\n')
            return result
        with patch('martin_agentic.release_bundle._files',side_effect=changing):
            with self.assertRaisesRegex(ValueError,'SOURCE_CHANGED'):self.build()
        self.assertFalse(self.output.exists())

    def test_existing_output_and_extraction_are_never_overwritten(self):
        self.build();before=(self.output/'manifest.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'OUTPUT_EXISTS'):self.build()
        with self.assertRaises(FileExistsError):verify_release_bundle(self.output,extract_to=self.root,**self.pins())
        self.assertEqual((self.output/'manifest.json').read_bytes(),before)

    def test_archive_tamper_and_untrusted_expected_source_rejected(self):
        self.build()
        with self.assertRaisesRegex(ValueError,'EXPECTED_SOURCE_MISMATCH'):
            verify_release_bundle(self.output,expected_source='f'*64)
        with (self.output/'source-and-tests.tar.gz').open('ab') as f:f.write(b'tampered')
        with self.assertRaisesRegex(ValueError,'ARCHIVE_HASH_MISMATCH'):verify_release_bundle(self.output)

    def test_path_traversal_rejected_before_creating_destination(self):
        self.build();m=self.manifest();m['files'][0]['path']='../escape';self.save_manifest(m)
        target=Path(self.tmp.name)/'destination'
        with self.assertRaisesRegex(ValueError,'UNSAFE_PATH'):verify_release_bundle(self.output,extract_to=target,**self.pins())
        self.assertFalse(target.exists())

    def test_symlink_tar_member_rejected_even_with_recomputed_archive_hash(self):
        self.build();m=self.manifest();path=self.output/'source-and-tests.tar.gz'
        with gzip.open(path,'wb') as zipped:
            with tarfile.open(fileobj=zipped,mode='w') as tar:
                row=m['files'][0];member=tarfile.TarInfo(row['path']);member.type=tarfile.SYMTYPE
                member.linkname='/etc/passwd';tar.addfile(member)
        m['archive_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();self.save_manifest(m)
        with self.assertRaisesRegex(ValueError,'ARCHIVE_MEMBER_INVALID'):verify_release_bundle(self.output)

    def test_manifest_cannot_hide_qualified_source_as_fixture(self):
        self.build();m=self.manifest()
        next(r for r in m['files'] if r['path']=='infra/src/martin_agentic/demo.py')['qualified_source']=False
        self.save_manifest(m)
        with self.assertRaisesRegex(ValueError,'SOURCE_FINGERPRINT_MISMATCH'):verify_release_bundle(self.output)

    def test_invalid_compression_is_a_classified_verification_failure(self):
        self.build();m=self.manifest();path=self.output/'source-and-tests.tar.gz'
        path.write_bytes(b'not a gzip archive')
        m['archive_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();self.save_manifest(m)
        with self.assertRaisesRegex(ValueError,'ARCHIVE_FORMAT_INVALID'):verify_release_bundle(self.output)

    def test_cli_requires_trusted_expected_source_without_opening_any_ledger(self):
        from martin_agentic.cli import main
        with (patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no ledger')),
                redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            result=main(['release-bundle','verify','--directory',str(self.output)])
        self.assertEqual(result,2)

    def test_dotenv_variants_and_private_key_names_are_not_packaged(self):
        for name in ('.env.production','.env.local','credentials.json','private.pem','id_ed25519'):
            with self.subTest(name=name):
                path=self.root/'infra/src/martin_agentic/scripts'/name
                path.parent.mkdir(parents=True,exist_ok=True);path.write_text('synthetic secret fixture')
                try:
                    with self.assertRaisesRegex(ValueError,'FORBIDDEN_INPUT'):self.build()
                    self.assertFalse(self.output.exists())
                finally:path.unlink()

    def test_extraction_requires_both_trusted_pins_before_creating_target(self):
        self.build();target=Path(self.tmp.name)/'target'
        for pins in ({},{'expected_source':self.manifest()['source_fingerprint']},
                {'expected_input_set_sha256':self.manifest()['input_set_sha256']}):
            with self.subTest(pins=pins):
                with self.assertRaisesRegex(ValueError,'EXTRACTION_TRUSTED_PINS_REQUIRED'):
                    verify_release_bundle(self.output,extract_to=target,**pins)
                self.assertFalse(target.exists())

    def test_forged_nonqualified_fixture_cannot_match_trusted_input_set_pin(self):
        self.build();trusted=self.pins();m=self.manifest();archive=self.output/'source-and-tests.tar.gz'
        payloads={}
        with tarfile.open(archive,'r:gz') as tar:
            for member in tar:
                with tar.extractfile(member) as stream:payloads[member.name]=stream.read()
        body=b'{"synthetic":true,"changed_after_tests":true}';payloads['examples/input.json']=body
        row=next(r for r in m['files'] if r['path']=='examples/input.json')
        row.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest())
        with gzip.open(archive,'wb') as zipped:
            with tarfile.open(fileobj=zipped,mode='w') as tar:
                for row in m['files']:
                    member=tarfile.TarInfo(row['path']);member.size=row['bytes'];member.mode=row['mode']
                    tar.addfile(member,io.BytesIO(payloads[row['path']]))
        m['archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();self.save_manifest(m)
        # Source-only diagnostics remain available, but cannot authorize extraction.
        self.assertEqual(verify_release_bundle(self.output,expected_source=trusted['expected_source'])['status'],
            'RELEASE_BUNDLE_VERIFIED')
        target=Path(self.tmp.name)/'forged-target'
        with self.assertRaisesRegex(ValueError,'EXPECTED_INPUT_SET_MISMATCH'):
            verify_release_bundle(self.output,extract_to=target,**trusted)
        self.assertFalse(target.exists())

    def test_wrong_manifest_and_row_types_are_classified_value_errors(self):
        self.build();manifest=self.manifest()
        for bad in ([],None,1,'manifest'):
            with self.subTest(value=bad):
                (self.output/'manifest.json').write_text(json.dumps(bad))
                with self.assertRaisesRegex(ValueError,'MANIFEST_INVALID'):verify_release_bundle(self.output)
        for badrow in ('row',[],None,{'path':'examples/input.json'}):
            with self.subTest(row=badrow):
                m={**manifest,'files':[badrow]};self.save_manifest(m)
                with self.assertRaisesRegex(ValueError,'FILE_METADATA_INVALID'):verify_release_bundle(self.output)
        for key in ('source_fingerprint','archive_sha256','input_set_sha256'):
            with self.subTest(hash=key):
                m={**manifest,key:123};(self.output/'manifest.json').write_text(json.dumps(m))
                with self.assertRaisesRegex(ValueError,'MANIFEST_HASH_INVALID'):verify_release_bundle(self.output)

    def test_oversized_source_is_rejected_without_unbounded_initial_fingerprint_read(self):
        source=self.root/'infra/src/martin_agentic/oversized.py'
        with source.open('wb') as handle:handle.truncate(1025)
        with (patch('martin_agentic.release_bundle.MAX_BYTES',1024),
                patch.object(Path,'read_bytes',side_effect=AssertionError('unbounded Path.read_bytes forbidden'))):
            with self.assertRaisesRegex(ValueError,'INPUT_TOO_LARGE'):self.build()
        self.assertFalse(self.output.exists())

    def test_fixture_changes_during_snapshot_and_before_publication_never_publish(self):
        from martin_agentic.release_bundle import _files
        fixture=self.root/'examples/input.json'
        for change_after in (1,2):
            with self.subTest(change_after=change_after):
                fixture.write_text('{"synthetic":true}');calls=[]
                def changing(root):
                    result=_files(root);calls.append(1)
                    if len(calls)==change_after:fixture.write_text('{"synthetic":true,"changed":true}')
                    return result
                with patch('martin_agentic.release_bundle._files',side_effect=changing):
                    with self.assertRaisesRegex(ValueError,'INPUT_SET_CHANGED'):self.build()
                self.assertFalse(self.output.exists())
