"""Default-off supervised engineering profile composition.

This is the single owner-side switch that binds the existing owner source,
formal review evidence reader and supervised-engineering authority to the
default Host bridge.  It does not create a permit, activate a real channel,
enable entries, or call a broker.  Real activation remains hard-disabled here;
the profile only makes the existing default path reach the exact named
activation/source rejection instead of ``SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE``.
"""
from pathlib import Path

PROFILE_KEY = 'supervised_engineering'
PROFILE_KIND = 'supervised_engineering_profile_v1'


def profile_config(policy):
    """Return enabled profile config or ``None``; malformed values fail closed."""
    value = policy.get(PROFILE_KEY) if isinstance(policy, dict) else None
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError('SUPERVISED_ENGINEERING_PROFILE_INVALID')
    if value.get('enabled') is not True:
        return None
    return dict(value)


def validate_profile_mode(policy, agent_mode, *, capture_only=False):
    """Reject a profile on a composition that cannot consume it."""
    config = profile_config(policy)
    if config is None:
        return None
    if agent_mode != 'supervised-host':
        raise ValueError(
            'SUPERVISED_ENGINEERING_PROFILE_REQUIRES_SUPERVISED_HOST')
    if capture_only is True:
        raise ValueError('SUPERVISED_ENGINEERING_PROFILE_CAPTURE_ONLY')
    return config


def _owner_root(value):
    if not isinstance(value, str) or not value:
        raise ValueError(
            'SUPERVISED_ENGINEERING_PROFILE_OWNER_ROOT_REQUIRED')
    path = Path(value)
    if path.is_symlink() or not path.is_dir():
        raise ValueError('SUPERVISED_ENGINEERING_PROFILE_OWNER_ROOT_INVALID')
    return str(path)


def review_config(config):
    """Return explicit review-only collection settings or ``None``.

    Review collection is a separate opt-in from the profile itself.  It is
    always review-only and bounded to the existing 10-second Host deadline.
    """
    review = config.get('review') if isinstance(config, dict) else None
    if review is None:
        return None
    if not isinstance(review, dict):
        raise ValueError('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    if review.get('enabled') is not True:
        return None
    if review.get('review_only') is not True:
        raise ValueError('SUPERVISED_ENGINEERING_REVIEW_ONLY_REQUIRED')
    deadline = review.get('deadline_seconds', 10)
    if (type(deadline) not in (int, float) or not 0 < deadline <= 10):
        raise ValueError('SUPERVISED_ENGINEERING_REVIEW_DEADLINE_INVALID')
    return {'enabled': True, 'review_only': True,
            'deadline_seconds': float(deadline)}


def review_settings(book):
    profile = getattr(book, 'supervised_engineering_profile', None)
    if not isinstance(profile, dict):
        return None
    return review_config(profile)


def permit_config(config):
    """Return explicit permit-production settings, default off."""
    permit = config.get('permit') if isinstance(config, dict) else None
    if permit is None:
        return None
    if not isinstance(permit, dict):
        raise ValueError('SUPERVISED_ENGINEERING_PERMIT_CONFIG_INVALID')
    if permit.get('enabled') is not True:
        return None
    return {'enabled': True}


def permit_settings(book):
    profile = getattr(book, 'supervised_engineering_profile', None)
    if not isinstance(profile, dict):
        return None
    return permit_config(profile)


def promotion_config(config):
    """Return explicit promotion-stage settings, default off.

    This is a local owner stage permission, not evidence that a formal broker
    channel exists.  The existing source verifier, current preflight, entry
    authority, control/risk gates and native confirmation remain mandatory.
    """
    promotion = config.get('promotion') if isinstance(config, dict) else None
    if promotion is None:
        return None
    if not isinstance(promotion, dict):
        raise ValueError('SUPERVISED_ENGINEERING_PROMOTION_CONFIG_INVALID')
    if promotion.get('enabled') is not True:
        return None
    return {'enabled': True}


def promotion_settings(book):
    profile = getattr(book, 'supervised_engineering_profile', None)
    if not isinstance(profile, dict):
        return None
    return promotion_config(profile)


def _optional_root(config, key):
    value = config.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError('SUPERVISED_ENGINEERING_PROFILE_' + key.upper()
                         + '_INVALID')
    path = Path(value)
    if path.is_symlink() or not path.is_dir():
        raise ValueError('SUPERVISED_ENGINEERING_PROFILE_' + key.upper()
                         + '_INVALID')
    return str(path)


def _bind_formal_authority(book, authority):
    """Expose the same authority to the default current preflight.

    There is no second authority or permit store: the existing object is
    attached to the already-created factory/preflight so a later refresh uses
    the same instance.
    """
    book.formal_authority = authority
    preflight = getattr(book, '_current_preflight', None)
    if preflight is not None and hasattr(preflight, 'formal_authority'):
        preflight.formal_authority = authority
    factory = getattr(book, '_current_context_factory', None)
    if factory is None:
        refresher = getattr(book, '_current_context_refresher', None)
        factory = getattr(refresher, 'factory', None)
    if factory is not None and hasattr(factory, 'formal_authority'):
        factory.formal_authority = authority


def configure_supervised_engineering_profile(
        book, policy=None, *, native_receipt_root=None,
        native_ingress_root=None):
    """Bind the default-off profile to the existing book authority.

    Returns the owner root when enabled, otherwise ``None``.  The profile is
    intentionally not a policy/entry switch: ``promotion.enabled`` is an
    explicit default-off local supervised-stage permission only.  It never
    proves formal channel capability, consumes a permit, claims or places.

    ``native_receipt_root``/``native_ingress_root`` allow the default Host
    caller to point the existing reader at its own private receipt/ingress
    directories when the policy does not name dedicated roots.
    """
    config = profile_config(policy if policy is not None else book.policy)
    if config is None:
        return None
    promotion = promotion_config(config)
    if book.simulation is not False:
        raise ValueError(
            'SUPERVISED_ENGINEERING_PROFILE_REQUIRES_REAL_BOOK')
    if book.policy.get('capture_only') is True:
        raise ValueError('SUPERVISED_ENGINEERING_PROFILE_CAPTURE_ONLY')
    owner_root = _owner_root(config.get('owner_root'))
    receipt_root = (_optional_root(config, 'native_receipt_root')
                    or (str(Path(native_receipt_root))
                        if native_receipt_root is not None else None))
    ingress_root = (_optional_root(config, 'native_ingress_root')
                    or (str(Path(native_ingress_root))
                        if native_ingress_root is not None else None))
    authority = book.configure_engineering_source(
        owner_root,
        owner_source_root=owner_root,
        native_receipt_root=receipt_root,
        native_ingress_root=ingress_root,
        allow_synthetic=False,
        # The authority's real_activation flag is only the local supervised
        # promotion stage permission.  It is default-off and never proves a
        # formal broker/MCP capability; source evidence/preflight/entry/risk/
        # native confirmation still decide each action.
        real_activation_enabled=promotion is not None)
    _bind_formal_authority(book, authority)
    book.supervised_engineering_profile = {
        'kind': PROFILE_KIND,
        'enabled': True,
        'owner_root': owner_root,
        'native_receipt_root': receipt_root,
        'native_ingress_root': ingress_root,
        'review': review_config(config),
        'permit': permit_config(config),
        'promotion': promotion,
        'real_activation_enabled': promotion is not None,
    }
    return owner_root
