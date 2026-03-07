import asyncio
import hashlib
import os
import platform
import socket
import ssl
import sys
import traceback
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

from kiwoom import MOCK, REAL, __version__ as kiwoom_version
from kiwoom.api import API


def load_env() -> str:
    try:
        from dotenv import find_dotenv, load_dotenv
    except Exception:
        print("[DIAG] python-dotenv 미설치 또는 import 실패. 현재 OS 환경변수만 사용합니다.")
        return "<dotenv-unavailable>"

    dotenv_path = find_dotenv(usecwd=True) or find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path, override=False)
        return dotenv_path
    load_dotenv(override=False)
    return "<not-found>"


def mask_secret(value: str, head: int = 4, tail: int = 4) -> str:
    value = str(value or "")
    if not value:
        return "<empty>"
    if len(value) <= head + tail:
        return f"{value[:1]}***{value[-1:]}"
    return f"{value[:head]}...{value[-tail:]}"


def ws_flags(value: str) -> tuple[bool, bool, bool]:
    value = str(value or "")
    has_leading = bool(value[:1] and value[:1].isspace())
    has_trailing = bool(value[-1:] and value[-1:].isspace())
    has_ctrl = any(ch in value for ch in ("\r", "\n", "\t"))
    return has_leading, has_trailing, has_ctrl


def value_fingerprint(value: str) -> str:
    value = str(value or "")
    if not value:
        return "<empty>"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def log_runtime_diag() -> None:
    print(
        "[RUNTIME] "
        f"python={sys.version.split()[0]}, platform={platform.platform()}, aiohttp={aiohttp.__version__}"
    )


def log_proxy_diag() -> None:
    keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ]
    parts: list[str] = []
    for key in keys:
        value = os.getenv(key)
        if not value:
            continue
        masked = value if len(value) <= 24 else f"{value[:12]}...{value[-8:]}"
        parts.append(f"{key}={masked}")
    if parts:
        print(f"[NET] proxy_env_present=True, {'; '.join(parts)}")
    else:
        print("[NET] proxy_env_present=False")


async def probe_dns_and_tls(host: str) -> None:
    parsed = urlparse(host)
    server = parsed.hostname or host
    port = parsed.port or 443

    try:
        infos = socket.getaddrinfo(server, port, proto=socket.IPPROTO_TCP)
        addrs = sorted({info[4][0] for info in infos})
        print(f"[NET] dns host={server}, port={port}, resolved_ips={addrs}")
    except Exception as e:
        print(f"[NET][DNS][ERROR] {type(e).__name__}: {e}")

    try:
        ssl_ctx = ssl.create_default_context()
        reader, writer = await asyncio.open_connection(server, port, ssl=ssl_ctx, server_hostname=server)
        ssl_obj = writer.get_extra_info("ssl_object")
        cert = ssl_obj.getpeercert() if ssl_obj else {}
        subject = cert.get("subject", "") if isinstance(cert, dict) else ""
        issuer = cert.get("issuer", "") if isinstance(cert, dict) else ""
        san = cert.get("subjectAltName", []) if isinstance(cert, dict) else []
        san_dns = [v for k, v in san if k == "DNS"][:5]
        print(
            "[NET] tls_handshake_ok=True, "
            f"cipher={ssl_obj.cipher() if ssl_obj else None}, "
            f"subject={subject}, issuer={issuer}, san_dns_sample={san_dns}"
        )
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"[NET][TLS][ERROR] {type(e).__name__}: {e}")


def detect_env_source(dotenv_path: str, key: str) -> str:
    # load_dotenv(override=False) means pre-exported shell env wins.
    if key in os.environ:
        if dotenv_path and dotenv_path not in {"<not-found>", "<dotenv-unavailable>"}:
            try:
                dotenv_text = Path(dotenv_path).read_text(encoding="utf-8", errors="ignore")
                if f"{key}=" in dotenv_text:
                    return "shell-env(overrides-dotenv)"
            except Exception:
                pass
        return "shell-env"
    return "dotenv-or-default"


async def probe_token_endpoint(host: str, appkey: str, secretkey: str) -> None:
    endpoint = f"{host}/oauth2/token"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "",
        "Accept": "application/json, text/plain, */*",
    }
    payload = {
        "grant_type": "client_credentials",
        "appkey": appkey,
        "secretkey": secretkey,
    }

    timeout = aiohttp.ClientTimeout(total=20)
    probe_variants = [
        (
            "default-ua",
            {
                **headers,
                "User-Agent": "Python/aiohttp kiwoom-auth-probe",
            },
        ),
        (
            "browser-like-ua",
            {
                **headers,
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
            },
        ),
    ]

    for name, probe_headers in probe_variants:
        try:
            async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                async with session.post(endpoint, headers=probe_headers, json=payload) as res:
                    text = await res.text()
                    compact = " ".join(text.split())
                    if len(compact) > 500:
                        compact = compact[:500] + "..."
                    interesting_headers = {
                        k: v
                        for k, v in res.headers.items()
                        if k.lower()
                        in {
                            "server",
                            "via",
                            "x-cache",
                            "x-request-id",
                            "cf-ray",
                            "content-type",
                            "content-length",
                        }
                    }
                    print(
                        "[PROBE] "
                        f"variant={name}, POST {endpoint} status={res.status}, reason={res.reason}, "
                        f"content_type={res.headers.get('Content-Type', '')}, "
                        f"resp_headers={interesting_headers}"
                    )
                    print(f"[PROBE] variant={name}, body={compact}")
        except Exception as e:
            print(f"[PROBE][ERROR] variant={name}, {type(e).__name__}: {e}")


async def main() -> None:
    dotenv_path = load_env()
    mode = os.getenv("KIWOOM_MODE", "mock")
    host_env = os.getenv("KIWOOM_HOST", "").strip()
    host = host_env or (REAL if mode == "real" else MOCK)
    appkey = os.getenv("KIWOOM_APPKEY", "")
    secretkey = os.getenv("KIWOOM_SECRETKEY", "")
    allowed_hosts = {REAL, MOCK}
    appkey_source = detect_env_source(dotenv_path, "KIWOOM_APPKEY")
    secret_source = detect_env_source(dotenv_path, "KIWOOM_SECRETKEY")

    app_lead, app_trail, app_ctrl = ws_flags(appkey)
    sec_lead, sec_trail, sec_ctrl = ws_flags(secretkey)
    print(
        "[DIAG] "
        f"dotenv_path={dotenv_path}, host={host}, "
        f"mode={mode}, kiwoom_version={kiwoom_version}, "
        f"appkey_source={appkey_source}, secret_source={secret_source}, "
        f"appkey(masked)={mask_secret(appkey)}, appkey_len={len(appkey)}, "
        f"appkey_fp={value_fingerprint(appkey)}, "
        f"appkey_leading_ws={app_lead}, appkey_trailing_ws={app_trail}, appkey_ctrl_char={app_ctrl}, "
        f"secret(masked)={mask_secret(secretkey)}, secret_len={len(secretkey)}, "
        f"secret_fp={value_fingerprint(secretkey)}, "
        f"secret_leading_ws={sec_lead}, secret_trailing_ws={sec_trail}, secret_ctrl_char={sec_ctrl}"
    )
    print(f"[DIAG] allowed_hosts={sorted(allowed_hosts)}")
    log_runtime_diag()
    log_proxy_diag()
    await probe_dns_and_tls(host)
    if host not in allowed_hosts:
        print(
            "[DIAG][WARN] host mismatch: "
            f"host={host} is not one of allowed_hosts; "
            "expected EXACT match, e.g. https://mockapi.kiwoom.com"
        )

    try:
        api = API(host=host, appkey=appkey, secretkey=secretkey)
    except Exception as e:
        print(f"[ERROR][INIT] {type(e).__name__}: {e}")
        print(
            "[ERROR][INIT] context="
            f"host={host}, mode={mode}, host_in_allowed={host in allowed_hosts}, "
            f"allowed_hosts={sorted(allowed_hosts)}"
        )
        traceback.print_exc()
        raise

    try:
        if os.getenv("KIWOOM_AUTH_PROBE", "1").lower() in {"1", "true", "yes", "y"}:
            await probe_token_endpoint(host=host, appkey=appkey, secretkey=secretkey)
        print("[TEST] connect() 시작")
        await api.connect()
        token = str(api.token() or "")
        print(f"[TEST] CONNECTED token={mask_secret(token, head=6, tail=4)}")

        body = await api.stock_list("0")  # 0=코스피
        count = len(body.get("list", [])) if isinstance(body, dict) else 0
        print(f"[TEST] stock_list count={count}")
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        cause = getattr(e, "__cause__", None)
        if cause is not None:
            print(f"[ERROR][CAUSE] {type(cause).__name__}: {cause}")
            status = getattr(cause, "status", None)
            message = getattr(cause, "message", None)
            url = getattr(cause, "request_info", None)
            if status is not None:
                print(f"[ERROR][CAUSE] status={status}, message={message}")
            if url is not None:
                try:
                    print(f"[ERROR][CAUSE] url={url.real_url}")
                except Exception:
                    pass
        traceback.print_exc()
    finally:
        await api.close()
        print("[TEST] close() 완료")


if __name__ == "__main__":
    asyncio.run(main())
