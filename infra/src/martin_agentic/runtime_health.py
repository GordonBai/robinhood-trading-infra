"""Observation health is separate from entry admission or broker authority.

This only interprets a controller observation. Callers displaying current
health must additionally verify process identity and heartbeat age.
"""


def health_scope(status):
    return 'read_only_capture' if status.get('read_only_capture') is True else 'lifecycle_observation'


def observation_healthy(status):
    if not isinstance(status, dict):
        return False
    if status.get('attention_required') or status.get('errors') or status.get('market_failure'):
        return False
    if status.get('read_only_capture') is True:
        if status.get('agent_mode') != 'disabled':
            return False
        if status.get('reason') == 'AUTHORIZED_SESSION_CLOSED':
            return (status.get('healthy') is True and status.get('final_flat_verified') is True
                    and status.get('final_broker_check_performed') is True
                    and status.get('close_disposition') == 'SAFE_FLAT')
        return status.get('read_only_capture_healthy') is True
    return status.get('healthy') is True
