from unittest import TestCase
from unittest.mock import patch
from subprocess import CompletedProcess

from martin_agentic.runtime_probe import probe_runtime


class RuntimeTests(TestCase):
    @patch('martin_agentic.runtime_probe.subprocess.run')
    def test_host_termination_is_not_reported_as_authentication_failure(self,run):
        run.return_value=CompletedProcess([],-9,'','')
        result=probe_runtime('/official/codex')
        self.assertEqual(result['blocker'],'HOST_TERMINATED_CLI')
        self.assertEqual(result['signal'],9)
        self.assertIsNone(result['configured_robinhood_server'])
    @patch('martin_agentic.runtime_probe.subprocess.run')
    def test_native_missing_is_not_misreported_as_no_auth(self,run):
        run.return_value=CompletedProcess([],1,'','ENOENT secret-token-example')
        result=probe_runtime('/fake/codex')
        self.assertEqual(result['blocker'],'NATIVE_EXECUTABLE_MISSING')
        self.assertNotIn('secret-token',str(result))
        self.assertEqual(run.call_count,1)

    @patch('martin_agentic.runtime_probe.subprocess.run')
    def test_configuration_is_not_authenticated_readiness(self,run):
        run.side_effect=[CompletedProcess([],0,'codex 0.118.0',''),
                         CompletedProcess([],0,'[{"name":"robinhood","auth_status":"oAuth","token":"secret"}]','')]
        result=probe_runtime('/fake/codex')
        self.assertTrue(result['configured_robinhood_server'])
        self.assertFalse(result['independent_authenticated_read_verified'])
        self.assertNotIn('secret',str(result))
