"""pykrx 웹 요청에 대해 공용 `requests.Session`을 활성화하는 유틸리티.

이 모듈은 `pykrx.website.comm.webio.requests`를 교체하여
`requests.Session()`을 사용하도록 변경합니다. 또한 환경변수에 로그인
정보가 있으면 KRX 자동 로그인으로 세션을 초기화합니다.

사용법: 애플리케이션 시작 시 가능한 일찍 `enable_pykrx_session()`을
호출하세요. 다른 모듈에서 `pykrx.stock`를 임포트하기 전에 호출해야
세션 패치가 적용됩니다.
"""
from __future__ import annotations

import os
from typing import Optional

import requests


def enable_pykrx_session(session: Optional[requests.Session] = None) -> requests.Session:
    """주어진 세션을 사용하거나 새 `requests.Session`을 생성하고
    pykrx 웹 IO 모듈이 이를 사용하도록 패치합니다.

    동작 요약:
    - `PYKRX_USER_AGENT`, `PYKRX_REFERER`로 기본 헤더를 덮어쓸 수 있습니다.
    - `PYKRX_LOGIN_ID`, `PYKRX_LOGIN_PW`가 있으면 자동 로그인(`login_krx`)을 시도합니다.
    - `PYKRX_SESSION_DEBUG`가 설정되어 있으면 디버그 정보를 출력합니다.

    반환값: 사용된 `requests.Session` 객체.
    """
    try:
        # pykrx가 설치되지 않은 경우에도 이 모듈을 임포트할 수 있도록 import 지연 처리합니다
        import pykrx.website.comm.webio as webio
    except Exception:
        webio = None

    sess = session or requests.Session()

    # pykrx에서 사용하는 공통 헤더를 적용합니다. 환경변수로 오버라이드할 수 있습니다
    ua = os.getenv("PYKRX_USER_AGENT")
    ref = os.getenv("PYKRX_REFERER")
    if ua:
        sess.headers.update({"User-Agent": ua})
    if ref:
        sess.headers.update({"Referer": ref})

    # pykrx 웹 IO의 모듈 전역에서 사용한 `requests` 이름을 이 세션으로 교체합니다.
    # 그러면 내부의 Get/Post 클래스가 `session.get`/`session.post`를 사용하게 됩니다.
    if webio is not None:
        try:
            setattr(webio, "requests", sess)
        except Exception:
            pass

    if os.getenv("PYKRX_SESSION_DEBUG"):
        print(f"[PYKRX_SESSION] enabled, cookies={len(sess.cookies)} headers={sess.headers}")

    # 환경 변수에 로그인 정보가 있으면 자동 로그인 시도를 합니다.
    try:
        _maybe_auto_login(sess)
    except Exception:
        pass

    return sess


def login_krx(session: requests.Session, login_id: str, login_pw: str) -> bool:
    """KRX(data.krx.co.kr)에 로그인하여 세션 쿠키(JSESSIONID)를 갱신합니다.

    로그인 흐름:
      1. GET MDCCOMS001.cmd  → 초기 JSESSIONID 발급
      2. GET login.jsp       → iframe 세션 초기화
      3. POST MDCCOMS001D1.cmd → 실제 로그인 시도
      4. 응답이 CD011(중복 로그인)일 경우에는 `skipDup=Y`를 추가하여 재전송

    성공 시 True(CD001), 실패 시 False를 반환합니다.
    """
    _LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
    _LOGIN_JSP = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
    _LOGIN_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    try:
        # 초기 요청으로 JSESSIONID 등 세션을 확보합니다.
        session.get(_LOGIN_PAGE, headers={"User-Agent": _UA}, timeout=15)
        session.get(_LOGIN_JSP, headers={"User-Agent": _UA, "Referer": _LOGIN_PAGE}, timeout=15)

        payload = {
            "mbrNm": "",
            "telNo": "",
            "di": "",
            "certType": "",
            "mbrId": login_id,
            "pw": login_pw,
        }
        headers = {"User-Agent": _UA, "Referer": _LOGIN_PAGE}

        resp = session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
        try:
            data = resp.json()
        except Exception:
            return False
        error_code = data.get("_error_code", "")

        # 중복 로그인 응답(CD011)이 오면 중복 확인 플래그를 추가하여 재시도합니다.
        # (서비스 흐름에 따라 CD001이 정상 코드입니다.)
        if error_code == "CD011":
            payload["skipDup"] = "Y"
            resp = session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
            try:
                data = resp.json()
            except Exception:
                return False
            error_code = data.get("_error_code", "")

        return error_code == "CD001"
    except Exception:
        return False


def _maybe_auto_login(sess: requests.Session) -> None:
    """환경변수에 로그인 정보가 있으면 자동으로 KRX 로그인 시도.

    - `PYKRX_LOGIN_ID`와 `PYKRX_LOGIN_PW`가 설정되어 있으면 `login_krx`를 호출합니다.
    - `PYKRX_SESSION_DEBUG`가 설정되어 있으면 결과를 출력합니다.
    """
    login_id = os.getenv("PYKRX_LOGIN_ID")
    login_pw = os.getenv("PYKRX_LOGIN_PW")
    if login_id and login_pw:
        ok = login_krx(sess, login_id, login_pw)
        if os.getenv("PYKRX_SESSION_DEBUG"):
            print(f"[PYKRX_SESSION] 자동 로그인 결과: {ok}")
