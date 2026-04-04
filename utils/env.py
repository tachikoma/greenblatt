"""환경변수 통합 유틸

프로젝트 전역에서 환경변수 우선순위를 통일하여 읽도록 합니다.
사용법:
    from utils.env import env_get

    num_stocks = int(env_get('NUM_STOCKS', fallback_keys=['BACKTEST_NUM_STOCKS','LIVE_NUM_STOCKS'], default='40'))
"""
from __future__ import annotations

import os
import warnings
from typing import Any


def env_get(key: str, *, fallback_keys: list[str] | None = None, default: Any = None, cast: callable | None = None) -> Any:
    """환경변수 우선 조회 유틸.

    우선 `key`를 조회하고, 값이 없으면 `fallback_keys` 순으로 조회합니다.
    구식 키가 발견되면 DeprecationWarning을 출력합니다.
    """
    val = os.getenv(key)
    if val not in (None, ""):
        return cast(val) if cast else val

    if fallback_keys:
        for fk in fallback_keys:
            fv = os.getenv(fk)
            if fv not in (None, ""):
                warnings.warn(f"환경변수 '{fk}'는 곧 deprecated 됩니다. 대신 '{key}'를 사용하세요.", DeprecationWarning)
                return cast(fv) if cast else fv

    return default
