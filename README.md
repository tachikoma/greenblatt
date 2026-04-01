# 그린블라트 응용 전략 - 한국 주식 백테스트/실거래

이 저장소는 한국 주식시장용 그린블라트 응용 전략을
- 백테스트(`greenblatt_korea_full_backtest.py`)
- 실거래/모의거래(`run_live_trading.py`)
로 분리해 운영합니다.

현재 문서는 실제 코드 기본값/옵션/환경변수에 맞춰 정리되어 있습니다.

---

## 빠른 시작

### 1) 의존성 설치

```bash
uv sync
```

### 2) 백테스트 실행

```bash
uv run greenblatt_korea_full_backtest.py
```

### 3) 실거래(또는 모의) 1회 실행

```bash
uv run run_live_trading.py --dry-run
```

---

## 핵심 동작 요약 (최신 코드 기준)

### 백테스트 기본 실행 프로파일

- 전략 모드: `mixed`
- 필터 프로파일: `large_cap`
- 종목 수: `40`
- 투자 비율: `0.95`
- 모멘텀: 활성화 (`momentum_months=3`, `momentum_weight=0.60`, `momentum_filter_enabled=true`)
- 리밸런싱: 월 단위 기본 `3개월` (`DEFAULT_REBALANCE_MONTHS=3`)

### 리밸런싱 주기 우선순위

- 일 단위: `--rebalance-days` > `REBALANCE_DAYS` > `LIVE_REBALANCE_DAYS`
- 월 단위: `--rebalance-months` > `REBALANCE_MONTHS` > `LIVE_REBALANCE_MONTHS` > 코드 기본값(3)
- `rebalance_days`가 설정되면 `rebalance_months`보다 우선 적용됩니다.

### 실거래 실행 흐름 요약

- 주기 가드(`LIVE_REBALANCE_GUARD_ENABLED`)로 동일 주기 중복 실행 방지
- 상태 파일(`LIVE_RUN_STATE_PATH`) 기반 상태머신 운용
  - `STARTED` -> `ORDER_SUBMITTED` -> `SUCCESS` 또는 `PARTIAL_PENDING`
  - 주문 전 실패: `FAILED_BEFORE_ORDER`
  - 주문 없음: `SKIPPED`
- `--dry-run` 시 실주문/상태저장 없이 신호/주문의도 계산만 수행

---

## CLI 사용법

### 백테스트

```bash
uv run greenblatt_korea_full_backtest.py
uv run greenblatt_korea_full_backtest.py --rebalance-months 1
uv run greenblatt_korea_full_backtest.py --rebalance-days 20
```

지원 인자:
- `--rebalance-months`, `-r`: 리밸런싱 주기(개월)
- `--rebalance-days`: 리밸런싱 주기(일, 설정 시 월 단위보다 우선)

### 실거래/모의거래

```bash
uv run run_live_trading.py
uv run run_live_trading.py --signal-date 2026-04-01
uv run run_live_trading.py --force
uv run run_live_trading.py --dry-run
```

지원 인자:
- `--signal-date`: 신호 기준일(`YYYY-MM-DD`)
- `--force`: 동일 주기라도 강제 실행
- `--dry-run`: 주문 없이 시뮬레이션

---

## 주요 환경변수

아래는 실제 코드에서 직접 참조하는 핵심 키들입니다.

### 공통/주기

```bash
REBALANCE_MONTHS=3
REBALANCE_DAYS=
LIVE_REBALANCE_MONTHS=3
LIVE_REBALANCE_DAYS=
```

### 백테스트

```bash
BACKTEST_START_DATE=2017-01-01
BACKTEST_END_DATE=2025-12-31
BACKTEST_INITIAL_CAPITAL=5000000
COMMISSION_FEE_RATE=0.0015
TAX_RATE=0.002

# 백테스트 변동성 타게팅
BACKTEST_VOL_TARGET_ENABLED=false
BACKTEST_VOL_TARGET_SIGMA=0.20
BACKTEST_VOL_TARGET_LOOKBACK=20
BACKTEST_VOL_TARGET_MIN_RATIO=0.30

# 백테스트 펀더멘털 소스
BACKTEST_FUNDAMENTAL_SOURCE=pykrx
```

### 실거래 핵심

```bash
KIWOOM_MODE=mock
KIWOOM_APPKEY=
KIWOOM_SECRETKEY=
KIWOOM_ACCOUNT_NO=

LIVE_INVESTMENT_RATIO=0.95
LIVE_NUM_STOCKS=40
LIVE_STRATEGY_MODE=mixed
LIVE_MIXED_FILTER_PROFILE=large_cap
LIVE_MOMENTUM_ENABLED=true
LIVE_MOMENTUM_MONTHS=3
LIVE_MOMENTUM_WEIGHT=0.60
LIVE_MOMENTUM_FILTER_ENABLED=true
LIVE_LARGE_CAP_MIN_MCAP=
LIVE_FUNDAMENTAL_SOURCE=kiwoom

LIVE_ORDER_TIMEOUT_MINUTES=3
LIVE_ORDER_PRICE_OFFSET_BPS=10
LIVE_MAX_RETRY_ROUNDS=5

LIVE_REBALANCE_GUARD_ENABLED=true
LIVE_RUN_STATE_PATH=results/live_state/rebalance_state.json
LIVE_RUN_LOCK_PATH=results/live_state/rebalance.lock

LIVE_DRY_RUN_ENABLED=false
```

### Kiwoom 엔드포인트/API ID

```bash
KIWOOM_ORDER_ENDPOINT=/api/dostk/ordr
KIWOOM_ORDER_API_ID=kt10000
KIWOOM_ORDER_BUY_API_ID=kt10000
KIWOOM_ORDER_SELL_API_ID=kt10001
KIWOOM_ORDER_MODIFY_API_ID=kt10002
KIWOOM_ORDER_CANCEL_API_ID=kt10003

KIWOOM_ORDER_STATUS_ENDPOINT=/api/dostk/acnt
KIWOOM_ORDER_STATUS_API_ID=kt00007
KIWOOM_ORDER_CANCEL_ENDPOINT=/api/dostk/ordr

KIWOOM_QUOTE_ENDPOINT=/api/dostk/mrkcond
KIWOOM_QUOTE_API_ID=ka10004
KIWOOM_QUOTE_MARKET_TYPE=0

KIWOOM_BALANCE_ENDPOINT=/api/dostk/acnt
KIWOOM_BALANCE_API_ID=kt00018
KIWOOM_DEPOSIT_API_ID=kt00001
```

### Kiwoom 종목/펀더멘털 조회 + 캐시

```bash
FUNDAMENTAL_SOURCE=auto
LIVE_FUNDAMENTAL_SOURCE=kiwoom
BACKTEST_FUNDAMENTAL_SOURCE=pykrx

KIWOOM_STOCK_LIST_ENDPOINT=/api/dostk/stkinfo
KIWOOM_STOCK_LIST_API_ID=ka10099
KIWOOM_STOCK_LIST_MAX_PAGES=50

KIWOOM_FUND_ENDPOINT=/api/dostk/stkinfo
KIWOOM_FUND_API_ID=ka10001
KIWOOM_FUND_CONCURRENCY=3
KIWOOM_FUND_MAX=0

KIWOOM_ALLOW_DATE_PROXY=false

KIWOOM_PREFILTER_ENABLED=true
KIWOOM_PREFILTER_TARGET=500
KIWOOM_PREFILTER_MIN_MCAP=50000000000
KIWOOM_PREFILTER_MIN_TRADING_VALUE=0
```

### 공통 요청 재시도 / pykrx

```bash
LIVE_COMMON_REQUEST_RETRIES=3
LIVE_COMMON_REQUEST_RETRY_BACKOFF_SECONDS=0.5
PYKRX_REQUEST_TIMEOUT=6.0
PYKRX_SESSION_DEBUG=0
PYKRX_LOGIN_ID=
PYKRX_LOGIN_PW=
PYKRX_USER_AGENT=Mozilla/5.0
PYKRX_REFERER=https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd
```

### 실거래 변동성 타게팅

```bash
LIVE_VOL_TARGET_ENABLED=false
LIVE_VOL_TARGET_SIGMA=0.20
LIVE_VOL_TARGET_LOOKBACK=20
LIVE_VOL_TARGET_MIN_RATIO=0.30
```

---

## 캐시 구조

기본 캐시 디렉터리: `results/cache`

- `industry_cache.json`
- `momentum_cache.json`
- `price_cache.json`
- `ticker_list_cache.json`
- `cache_meta.json`
- `fundamentals/` (`parquet` 우선, 실패 시 `csv` 폴백)

캐시 버전/핵심 파라미터가 바뀌면, 비호환 캐시는 자동 무효화됩니다.

---

## 실거래 운영 참고

- 모의서버/실서버 모두 API 키는 필수입니다.
- `KIWOOM_MODE=real` 운영 시 고정 IP 및 화이트리스트 구성을 권장합니다.
- `--dry-run`으로 신호/주문의도만 먼저 검증한 뒤 실주문 실행을 권장합니다.

---

## 테스트

```bash
uv run pytest -q
```

Kiwoom mock 통합 테스트:

```bash
export KIWOOM_MODE=mock
export KIWOOM_APPKEY="<your-mock-appkey>"
export KIWOOM_SECRETKEY="<your-mock-secret>"
export RUN_KIWOOM_MOCK_TESTS=1
PYTHONPATH=. uv run pytest -q -m integration
```

---

## 주의사항

- 본 코드는 연구/학습 목적이며 투자 손실에 대한 책임은 사용자에게 있습니다.
- 과거 성과는 미래 수익을 보장하지 않습니다.
- 실거래 적용 전 모의투자/소액 검증을 권장합니다.
