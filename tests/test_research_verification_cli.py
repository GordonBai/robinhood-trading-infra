"""A verification success is reproducibility only, never trading admission."""
from contextlib import ExitStack, redirect_stdout, redirect_stderr
import io,json
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.cli import main


class ResearchVerificationCliTests(TestCase):
    def test_verify_is_readonly_delegation_without_book_or_broker_or_new_run(self):
        with ExitStack() as stack:
            verify=stack.enter_context(patch('martin_luk.research_verification.verify_research_run',return_value={
                'status':'REPRODUCIBLE','reproducible':True,'research_admitted':False,'production_ready':False}))
            book=stack.enter_context(patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no trading book')))
            replay=stack.enter_context(patch('martin_luk.research.run_research',side_effect=AssertionError('no new registered run')))
            collect=stack.enter_context(patch('martin_agentic.broker.collect_broker',side_effect=AssertionError('no network')))
            out,err=io.StringIO(),io.StringIO();stack.enter_context(redirect_stdout(out));stack.enter_context(redirect_stderr(err))
            code=main(['research-verify','--registry','runs.sqlite','--run-id','exact-run','--manifest','frozen.json','--report','original.json'])
            self.assertEqual(code,0,err.getvalue());result=json.loads(out.getvalue())
            self.assertFalse(result['research_admitted']);self.assertFalse(result['production_ready'])
            verify.assert_called_once_with('runs.sqlite','exact-run','frozen.json',report_path='original.json')
            book.assert_not_called();replay.assert_not_called();collect.assert_not_called()

    def test_unverified_or_truthy_string_result_is_failed_cli_exit(self):
        for value in (False,None,'true',1):
            with self.subTest(value=value), ExitStack() as stack:
                stack.enter_context(patch('martin_luk.research_verification.verify_research_run',return_value={
                    'status':'VERIFICATION_FAILED','reproducible':value,'research_admitted':False}))
                stack.enter_context(redirect_stdout(io.StringIO()));stack.enter_context(redirect_stderr(io.StringIO()))
                code=main(['research-verify','--registry','runs.sqlite','--run-id','exact-run','--manifest','frozen.json'])
                self.assertEqual(code,2)
