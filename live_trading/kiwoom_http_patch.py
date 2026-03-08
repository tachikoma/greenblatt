from __future__ import annotations

import os
from typing import Any

from kiwoom.http import client as kiwoom_client_mod


_ORIGINAL_CLIENT_SESSION = None
_PATCH_ACTIVE = False


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def apply_kiwoom_client_session_patch(
    *,
    user_agent: str | None = None,
    accept: str | None = None,
    trust_env: bool | None = None,
) -> bool:
    """Monkey patch kiwoom ClientSession construction to enforce stable headers."""
    global _ORIGINAL_CLIENT_SESSION, _PATCH_ACTIVE

    if _PATCH_ACTIVE:
        return False

    ua = (user_agent or os.getenv("KIWOOM_HTTP_USER_AGENT", "KiwoomClient/1.0")).strip()
    accept_header = (accept or os.getenv("KIWOOM_HTTP_ACCEPT", "application/json, text/plain, */*")).strip()
    resolved_trust_env = _env_bool("KIWOOM_HTTP_TRUST_ENV", True) if trust_env is None else trust_env

    _ORIGINAL_CLIENT_SESSION = kiwoom_client_mod.ClientSession

    def _patched_client_session(*args: Any, **kwargs: Any):
        headers = dict(kwargs.get("headers") or {})
        if ua:
            headers.setdefault("User-Agent", ua)
        if accept_header:
            headers.setdefault("Accept", accept_header)
        kwargs["headers"] = headers
        kwargs.setdefault("trust_env", resolved_trust_env)
        return _ORIGINAL_CLIENT_SESSION(*args, **kwargs)

    kiwoom_client_mod.ClientSession = _patched_client_session
    _PATCH_ACTIVE = True
    print(
        "[KIWOOM][PATCH] ClientSession patch enabled: "
        f"user_agent={ua}, accept={accept_header}, trust_env={resolved_trust_env}"
    )
    return True


def revert_kiwoom_client_session_patch() -> bool:
    global _ORIGINAL_CLIENT_SESSION, _PATCH_ACTIVE

    if not _PATCH_ACTIVE or _ORIGINAL_CLIENT_SESSION is None:
        return False

    kiwoom_client_mod.ClientSession = _ORIGINAL_CLIENT_SESSION
    _ORIGINAL_CLIENT_SESSION = None
    _PATCH_ACTIVE = False
    return True
