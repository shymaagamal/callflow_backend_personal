# callflow/core/utils/permissions.py
"""
Group-based permission helpers for Callflow views.
"""

import logging
from .logging_utils import build_request_logger, get_request_identity   # ← Add this

logger = logging.getLogger(__name__)

DOWNLOAD_GROUP = "callflow_download_files"


def get_user_groups(request) -> list:
    """Robust group extraction from LDAP + Session."""
    groups = []

    # 1. Django authenticated user
    user = getattr(request, 'user', None)
    if user and getattr(user, 'is_authenticated', False):
        try:
            if hasattr(user, 'ldap_user') and hasattr(user.ldap_user, 'group_names'):
                groups.extend(user.ldap_user.group_names)
            elif hasattr(user, 'groups'):
                groups.extend(user.groups.values_list('name', flat=True))
        except Exception as e:
            logger.debug(f"LDAP/Django groups error: {e}")

    # 2. Session (your Angular login flow)
    if not groups:
        try:
            session_groups = request.session.get('user_groups', [])
            for g in session_groups:
                if isinstance(g, dict) and 'group' in g:
                    groups.append(g['group'])
                else:
                    groups.append(str(g))
        except Exception as e:
            logger.debug(f"Session groups error: {e}")

    return list(dict.fromkeys(groups))


def can_download_files(request) -> bool:
    """Permission check for downloads — now also logs to per-request file."""
    
    # Build request-aware logger so permission logs go into the job's .txt file
    logger.info(f"[PERMISSION CHECK] Checking download permission for request: {request}")
    try:
        request_id = getattr(request, 'request_id', 'perm-check')  # fallback
        who = get_request_identity(request)
        request_logger = build_request_logger(None, request_id, who)  # None = no file yet
    except:
        request_logger = logger  # fallback

    user = getattr(request, 'user', None)
    is_auth = getattr(user, 'is_authenticated', False)

    request_logger.info(f"[PERMISSION] User: {user} | Authenticated: {is_auth}")

    if not is_auth:
        request_logger.warning("[PERMISSION] Unauthenticated user tried to download")
        return False

    groups = get_user_groups(request)
    request_logger.info(f"[PERMISSION] Groups found: {groups}")

    allowed = DOWNLOAD_GROUP in groups

    if not allowed:
        request_logger.warning(
            f"[PERMISSION DENIED] Missing '{DOWNLOAD_GROUP}' group. Has: {groups}"
        )
        
    return allowed