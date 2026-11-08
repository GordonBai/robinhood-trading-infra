"""Inspect official CLI readiness without reading/copying auth storage."""

import json
import shutil
import subprocess


def probe_runtime(binary=None):
    path=binary or shutil.which('codex')
    result={'runtime_available':False,'configured_robinhood_server':None,
            'independent_authenticated_read_verified':False,'live_execution_qualified':False}
    if not path:
        return {**result,'blocker':'CODEX_CLI_NOT_FOUND'}
    try:
        version=subprocess.run([path,'--version'],capture_output=True,text=True,timeout=10)
        if version.returncode:
            if version.returncode<0:
                return {**result,'blocker':'HOST_TERMINATED_CLI','signal':-version.returncode}
            return {**result,'blocker':'NATIVE_EXECUTABLE_MISSING' if 'ENOENT' in version.stderr else 'CLI_START_FAILED'}
        result['runtime_available']=True
        result['version']=version.stdout.strip()[:120]
        response=subprocess.run([path,'mcp','list','--json'],capture_output=True,text=True,timeout=15)
        if response.returncode:
            return {**result,'blocker':'MCP_CONFIG_LIST_FAILED'}
        data=json.loads(response.stdout)
        matches=[row for row in data if isinstance(row,dict) and 'robinhood' in row.get('name','').lower()]
        result['configured_robinhood_server']=bool(matches)
        result['blocker']='AUTHENTICATED_READ_STILL_REQUIRED' if matches else 'ROBINHOOD_MCP_NOT_CONFIGURED'
        return result
    except subprocess.TimeoutExpired:
        return {**result,'blocker':'CLI_TIMEOUT'}
    except (OSError,ValueError,TypeError):
        return {**result,'blocker':'CLI_PROBE_FAILED'}
