# 그린블라트 응용 전략 - 한국 주식 백테스트

## 전략 개요

네이버 카페에 소개된 그린블라트(Greenblatt) 공식의 응용 전략을 한국 주식시장에 적용한 백테스트입니다.

### 핵심 전략

1. **매수 조건**
   - 시장: KOSPI + KOSDAQ 통합
   - 시가총액 하위 50% (소형~중형주)
   - PER > 0, PBR > 0 (극단적 저평가 제외: PER<100, PBR<5)
   - 시가총액 최소 1,000억원 이상 (유동성 고려)
   - 금융주 포함 (은행, 보험 등도 선택)
   - 가치 순위: PER + PBR 순위 합산 (낮을수록 가치 있음)
   - 수익성 순위: ROE(자기자본이익률) 순위 (높을수록 좋음, 최대 100%로 제한)
   - 종합 순위: 가치 + 수익성 순위 합산으로 상위 N개 선정
   - 자산의 60%만 투자 (40% 현금 보유)
   - 연 1회(1월 1일) 리밸런싱
   - 38~47개 종목 분산 투자

### 현재 백테스트 결과 (2017~2024)

| 지표 | 값 |
|------|-----|
| 초기 자본 | 1,000,000원 |
| 최종 자산 | 970,439원 |
| 총 수익률 | **-2.96%** |
| CAGR | **-0.37%** |
| MDD | -2.89% |
| 승률 | 37.50% |
| 샤프 비율 | -1.01 |
| 거래 횟수 | 474회 |

#### 분석

**선정된 종목의 특징:**
- PER: 2.5~4.5배 (평균), 범위: 0.0~7.2배
- PBR: 0.45~0.74배 (평균), 범위: 0.14~1.74배  
- ROE: 17~25% (평균), 범위: 5~100%

**문제점:**
1. 극저평가 종목들은 시장이 회피하는 이유가 존재 (구조적 부실, 손절매 위험)
2. 소형주 중심 → 유동성 부족, 높은 거래 거래 비용 영향
3. 한국 증시의 강한 기술주 랠리에 대응 못함 (저PER = 경기 부진 업종)
4. 2017년 이후 고배당 저성장 종목들의 실적 악화

#### 권장사항

**성과 개선을 위해 시도할 사항:**
- [ ] 모멘텀 필터 추가 (상승 추세만 선택)
- [ ] 기술주/성장주 포함 (PER이 높아도 성장성 있는 종목)
- [ ] 분기별 리밸런싱 (연 1회 → 4회로 증가)
- [ ] 손절매 규칙 강화 (현재 1년 이상 손실만 매도)
- [ ] 선별된 산업(금융, 유틸리티) 회피 필터 추가
- [ ] 벤치마크 대비 상대 강도(Relative Strength) 필터 추가

---

## 설치 방법

> **패키지 매니저: [uv](https://docs.astral.sh/uv/)**

### 1. uv 설치 (미설치 시)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 의존성 설치 및 가상환경 생성

```bash
uv sync
```

### 3. Git hooks 온보딩 (팀 공통, 1회)

```bash
bash scripts/bootstrap-hooks.sh
```

- `core.hooksPath`를 `.githooks`로 설정해 `pre-commit`, `pre-push` 훅을 활성화합니다.
- 워크플로 변경 시 `actionlint`로 사전 검사를 수행합니다.

> `actionlint` 미설치 시(macOS): `brew install actionlint`

### 4. 파일 구조

```
.
├── greenblatt_korea_full_backtest.py      # 백테스트 실행 스크립트
├── visualize_backtest.py                  # 결과 시각화
├── pyproject.toml                         # 프로젝트 및 의존성 설정
├── uv.lock                                # 의존성 잠금 파일
└── README.md                              # 이 파일

## 새로운 옵션: `mixed_filter_profile='large_cap'`

- 설명: 기존의 상대적/소형주 중심 필터 대신 시가총액 상위 기업을 우선 선별하는 프로파일입니다. 대형주 중심의 샘플을 얻고 싶을 때 사용합니다.
- 동작: 시장 전체에서 시가총액 상위 20% 컷을 적용한 뒤, 추가로 `large_cap_min_mcap` 파라미터로 하한 시가총액(원 단위)을 설정할 수 있습니다.
- 동작: 시장 전체에서 시가총액 상위 20% 컷을 적용한 뒤, 추가로 `large_cap_min_mcap` 파라미터로 하한 시가총액(원 단위)을 설정할 수 있습니다. (기본값: `None` → None이면 top20만 적용)
- 사용 예시:

```python
backtest = KoreaStockBacktest(
    mixed_filter_profile='large_cap',
    large_cap_min_mcap=1e11  # 시가총액 최소 1천억원
)
```

Note: `large_cap_min_mcap` 기본값은 `None`입니다. `None`일 경우 기본 동작은 시가총액 상위 20%(top20 기반)만 적용하며, 숫자를 설정하면 해당 값을 하한으로 추가 적용합니다; 값 변경 시 백테스트 결과가 달라질 수 있습니다.
```

---

## 사용 방법

### 기본 실행

```bash
uv run greenblatt_korea_full_backtest.py
```

### `.env` / `.env.sample` 키 동기화 점검

```bash
python scripts/check_env_keys.py
python scripts/check_env_keys.py --strict
```

- 기본 실행: 키 차이 리포트 출력, 종료코드 0
- `--strict`: 키 차이가 있으면 종료코드 1 (CI/훅 연동용)

### Kiwoom 데이터 소스 사용 가이드 (ka10099 + 캐시 + 날짜 일관성)

`stock_selector.py`는 펀더멘털/시총 조회 시 다음 우선순위로 동작합니다.

1. 메모리 캐시(`{date}|{market}`)
2. 디스크 캐시(`results/cache/fundamentals/*`)
3. Kiwoom (`ka10099` 종목리스트 + 종목별 펀더멘털)
4. 실패 시 pykrx fallback

추가로 종목리스트(`ka10099`)는 **연속조회(cont-yn/next-key)** 를 사용하며,
날짜+시장 단위로 `results/cache/ticker_list_cache.json`에 캐시됩니다.

권장 환경변수:

```bash
# ka10099 endpoint/api-id
KIWOOM_STOCK_LIST_ENDPOINT=/api/dostk/stkinfo
KIWOOM_STOCK_LIST_API_ID=ka10099

# ka10099 연속조회 최대 페이지 수 (기본 50)
KIWOOM_STOCK_LIST_MAX_PAGES=50

# 과거 날짜를 Kiwoom 현재 스냅샷으로 대체 허용 여부 (기본 false)
# false: 과거 날짜는 pykrx 우선(일관성 보수적)
# true : 과거 날짜도 Kiwoom 사용 가능(속도/가용성 우선)
KIWOOM_ALLOW_DATE_PROXY=false

# 펀더멘털 데이터 소스 분리 스위치
# 값: auto | kiwoom | pykrx
LIVE_FUNDAMENTAL_SOURCE=kiwoom
BACKTEST_FUNDAMENTAL_SOURCE=pykrx
FUNDAMENTAL_SOURCE=auto

# 종목별 Kiwoom 펀더멘털 병렬 조회 옵션
# 0이면 전체, 양수면 상위 N개 티커만 조회
KIWOOM_FUND_MAX=0
KIWOOM_FUND_CONCURRENCY=12

# 1차 프리필터(유동성/시총)로 Kiwoom 종목별 조회 대상 축소
# true면 pykrx 시총/거래대금 데이터를 이용해 상위 후보만 남김
KIWOOM_PREFILTER_ENABLED=true

# 프리필터 후 유지할 최대 티커 수 (시장별)
KIWOOM_PREFILTER_TARGET=500

# 프리필터 최소 시가총액(원)
KIWOOM_PREFILTER_MIN_MCAP=50000000000

# 프리필터 최소 거래대금(원), 0이면 미적용
KIWOOM_PREFILTER_MIN_TRADING_VALUE=0

# 참고: 프리필터 스냅샷 조회가 실패해도 TARGET 값은 안전 하드캡으로 적용됩니다.
```

운영 권장값:

- 백테스트(과거 데이터 정확성 우선): `KIWOOM_ALLOW_DATE_PROXY=false`
- 실거래/당일 리밸런싱(응답성 우선): `KIWOOM_ALLOW_DATE_PROXY=true`
- 백테스트 소스(권장): `BACKTEST_FUNDAMENTAL_SOURCE=pykrx`
- 실거래 소스(권장): `LIVE_FUNDAMENTAL_SOURCE=kiwoom`

동작 참고:

- `KIWOOM_ALLOW_DATE_PROXY=false`이면 과거일자 요청에서 Kiwoom 경로를 건너뛰고 pykrx로 폴백합니다.
- `KIWOOM_ALLOW_DATE_PROXY=true`이면 과거일자라도 Kiwoom 현재 스냅샷을 사용하며, 로그에 date proxy 안내가 출력됩니다.
- `BACKTEST_FUNDAMENTAL_SOURCE=pykrx`이면 백테스트에서 Kiwoom 호출을 건너뛰고 pykrx만 사용합니다.
- `LIVE_FUNDAMENTAL_SOURCE=kiwoom`이면 실거래에서 Kiwoom 우선 조회 후 필요 시 pykrx로 폴백합니다.
- 캐시 버전/파라미터 불일치 시 기존 캐시는 자동 무효화됩니다.

### 실거래 자동매매(초기 구현)

`kiwoom-restful` 기반으로 백테스트 전략 산출을 실거래 주문으로 연결하는 초기 실행 스크립트가 추가되었습니다.

1. 환경 변수 설정

```bash
cp .env.sample .env
# .env 에서 KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_MODE 등 설정
```

1. 1회 리밸런싱 실행

```bash
uv run run_live_trading.py
```

이미 실행한 동일 주기를 강제로 재실행하려면:

```bash
uv run run_live_trading.py --force
```

### GitHub Actions로 주기 실행

워크플로 파일: `.github/workflows/live-trading-manual.yml`

- 실행 방식: 수동 실행 전용 (`workflow_dispatch`)
- 수동 실행: Actions 탭에서 `Run workflow`로 `signal_date`, `force` 입력 가능
- 러너는 일회성이므로 `results/live_state`를 캐시 복원/저장해 `LIVE_REBALANCE_GUARD_ENABLED` 상태를 유지

필수 Repository Secrets:

- `KIWOOM_APPKEY`
- `KIWOOM_SECRETKEY`

권장 Repository Secrets:

- `KIWOOM_MODE` (기본 mock 권장)
- `KIWOOM_ACCOUNT_NO`

선택 Repository Variables (`Settings > Secrets and variables > Actions > Variables`):

- `LIVE_REBALANCE_MONTHS`, `LIVE_NUM_STOCKS`, `LIVE_INVESTMENT_RATIO`
- `LIVE_ORDER_TIMEOUT_MINUTES`, `LIVE_ORDER_PRICE_OFFSET_BPS`, `LIVE_MAX_RETRY_ROUNDS`
- `KIWOOM_ORDER_*`, `KIWOOM_ORDER_STATUS_*`, `KIWOOM_ORDER_CANCEL_*`, `KIWOOM_QUOTE_*`

주의:

- private repository 비용 리스크를 줄이기 위해 github-hosted 워크플로는 스케줄을 비활성화했습니다.
- 스케줄 워크플로는 기본 브랜치에서만 동작하며, 장기간 저장소 활동이 없으면 자동 비활성화될 수 있습니다.
- 키움 REST API는 호출 IP 화이트리스트 등록이 필요하므로, **실거래(`KIWOOM_MODE=real`)는 고정 IP가 있는 self-hosted runner에서만 실행**하세요.
- 현재 워크플로는 `KIWOOM_MODE=real` + `github-hosted` 조합이면 안전하게 실패하도록 가드가 포함되어 있습니다.

### 실거래 전용 self-hosted 워크플로

워크플로 파일: `.github/workflows/live-trading-real-selfhosted.yml`

- 목적: 키움 화이트리스트 IP가 등록된 **self-hosted runner 전용** 실거래 실행
- 러너 라벨: `self-hosted`, `linux`, `arm64`, `kiwoom-real`
- 동시실행 방지: `concurrency.group=live-trading-real-selfhosted` (`cancel-in-progress: false`)
- 모드 고정: `KIWOOM_MODE=real` (워크플로 내부 고정)

필수 Repository Secrets:

- `KIWOOM_APPKEY`
- `KIWOOM_SECRETKEY`
- `KIWOOM_ACCOUNT_NO`

선택 Repository Secrets:

- `TELEGRAM_BOT_TOKEN` (성공/실패 알림)
- `TELEGRAM_CHAT_ID` (성공/실패 알림)

운영 팁:

- self-hosted runner 서비스 계정에 고정 공인 IP를 부여하고, 해당 IP를 키움 API 화이트리스트에 등록하세요.
- 워크플로는 런타임 상태를 `.runtime/live_state`에 저장해 동일 주기 중복 실행 가드를 유지합니다.
- 런타임 보고서는 `.runtime/live_reports`에 저장되고 아티팩트로 업로드됩니다.

주의:

- 기본값은 `KIWOOM_MODE=mock` 입니다.
- 운영 권장: 외부 스케줄러(cron/launchd 등)로 평일 1회 트리거하고, 내부 주기 가드(`LIVE_REBALANCE_GUARD_ENABLED`)로 동일 주기 중복 실행을 차단하세요.
- 내부 상태 머신(`execution_state`) 전이: `STARTED` → `ORDER_SUBMITTED` → (`SUCCESS` | `PARTIAL_PENDING`) / 주문 전 실패 시 `FAILED_BEFORE_ORDER` / 주문 없음은 `SKIPPED`.
- 동일 주기 재실행 판단: `SUCCESS`/`SKIPPED`는 스킵, `FAILED_BEFORE_ORDER`는 재실행 허용, `ORDER_SUBMITTED`/`PARTIAL_PENDING`은 신규 주문 없이 `reconcile_only`로 종료.
- 주문 엔드포인트/`api-id`는 계좌/상품 설정에 따라 다를 수 있어 `.env`의 `KIWOOM_ORDER_ENDPOINT`, `KIWOOM_ORDER_API_ID`로 조정하도록 구현되어 있습니다.
- 현재 구현 상태머신: 개장 시각(`LIVE_MARKET_OPEN_HHMM`) 대기(+grace second) → 1차 지정가 주문(`LIVE_ORDER_PRICE_OFFSET_BPS`) → `LIVE_ORDER_TIMEOUT_MINUTES` 대기 후 미체결 조회/취소/재주문을 최대 `LIVE_MAX_RETRY_ROUNDS`회 반복 → 최종 체결 확인 라운드.
- 미체결 조회/취소 엔드포인트는 `.env`의 `KIWOOM_ORDER_STATUS_*`, `KIWOOM_ORDER_CANCEL_*`로 계좌 스펙에 맞게 조정해야 합니다.
- 재주문 가격은 기본적으로 최우선 호가 기반입니다(`LIVE_USE_HOGA_RETRY_PRICE=true`): BUY는 최우선 매도호가, SELL은 최우선 매수호가를 사용합니다.
- 호가 조회 실패 시 `LIVE_RETRY_PRICE_OFFSET_BPS` 기반 폴백 가격을 사용하며, 호가 API는 `.env`의 `KIWOOM_QUOTE_*`로 조정할 수 있습니다.
- 계좌별 응답 필드 차이 확인이 필요하면 `LIVE_LOG_QUOTE_RESPONSE=true`로 설정하면 호가 조회 원본 응답을 최초 1회 로그로 출력합니다.
- 일일 체결 리포트는 `LIVE_SAVE_DAILY_REPORT=true`일 때 `LIVE_REPORT_DIR` 아래 `fills_YYYYMMDD.csv`로 저장됩니다.
- 중복 실행 방지 상태는 `LIVE_RUN_STATE_PATH`, 동시 실행 락은 `LIVE_RUN_LOCK_PATH`에 저장됩니다.
- `LIVE_REBALANCE_MONTHS` 기준 주기 키(예: 3개월 주기)를 계산해 동일 주기 재실행을 차단하며, 긴급 재실행이 필요할 때만 `--force`를 사용하세요.
- 선정 종목/주문 의도 디버그 출력이 필요하면 `LIVE_DEBUG_SIGNAL_ENABLED=true`로 설정하세요. 출력 길이는 `LIVE_DEBUG_MAX_ROWS`로 제한할 수 있습니다.

### OpenDART API 키 설정 (권장)

실제 재무제표 기반으로 `매출총이익`, `총자산`, `부채비율`을 반영하려면 OpenDART API 키가 필요합니다.

```bash
export DART_API_KEY="여기에_발급받은_API_KEY"
uv run greenblatt_korea_full_backtest.py
```

> 날짜 기준 안내: 리밸런싱/매수/매도/성과 기록은 **실행일(영업일) 기준**입니다.
> (예: 1월 1일이 휴장일이면 다음 영업일로 자동 이월되어 CSV/출력에 기록)
> (종료일이 휴장일이면 최종 평가는 **이전 영업일** 기준으로 계산)

### 커스텀 설정

```python
from greenblatt_korea_full_backtest import KoreaStockBacktest

# 백테스트 객체 생성
backtest = KoreaStockBacktest(
    start_date='2018-01-01',    # 시작일
    end_date='2023-12-31',      # 종료일
    initial_capital=10000000,   # 초기 자본 (1000만원)
    investment_ratio=0.6,       # 투자 비율 (60%)
    num_stocks=30               # 보유 종목 수 (30개)
)

# 백테스트 실행
results = backtest.run_backtest()

# 결과 출력
backtest.print_results(results)
```

---

## 코드 설명

### 1. 데이터 수집

```python
def get_fundamental_data(self, date, tickers):
    """종목별 재무 데이터 수집"""
    # pykrx로 PER/PBR/시가총액 수집
    # OpenDART로 매출총이익/총자산/부채비율 병합
```

### 2. 종목 스크리닝

```python
def screen_stocks_pykrx_roe(self, target_date):
    """pykrx ROE 기반 종목 스크리닝 (최신 핵심 로직)"""
    # 1. PER, PBR, EPS, BPS 수집 (pykrx)
    # 2. 금융주 필터링
    # 3. ROE(자기자본이익률) = EPS / BPS × 100 계산
    # 4. 등수 기반 복합 순위 (PER등수 + PBR등수 + ROE등수)
    # 5. 상위 N개 선정
```

### 3. 포트폴리오 관리

```python
def rebalance(self, selected_stocks, rebalance_date):
    """연 1회 리밸런싱"""
    # 기존 종목 전량 매도
    # 새로운 종목 균등 매수

def sell_losers(self, current_date):
    """1년 보유 후 손실 종목 매도"""
    # 보유 기간 확인
    # 수익률 < 0 종목 매도
```

---

## 출력 결과

### 콘솔 출력

```
================================================================================
백테스트 결과
================================================================================
초기 자본:           1,000,000원
최종 자산:           5,234,567원
총 수익률:              423.46%
CAGR:                    32.15%
MDD:                    -18.23%
승률:                    75.50%
백테스트 기간:             5.00년
총 거래 횟수:                 120회
================================================================================

연도별 누적 수익률:
----------------------------------------
2018:      28.50%
2019:      65.30%
2020:     125.80%
2021:     285.40%
2022:     350.20%
2023:     423.46%
----------------------------------------
```

### CSV 파일

1. **backtest_portfolio.csv**: 날짜별 포트폴리오 가치 변화 (**실행일/영업일 기준 날짜**)
2. **backtest_trades.csv**: 모든 매수/매도 거래 내역 (**실행일/영업일 기준 날짜**)

---

## 주요 함수 설명

### KoreaStockBacktest 클래스

| 메서드 | 설명 |
|--------|------|
| `get_kospi_tickers()` | KOSPI 상장 종목 리스트 조회 |
| `get_fundamental_data()` | 재무제표 데이터 수집 |
| `calculate_composite_score()` | 가치+수익성 복합 점수 계산 |
| `screen_stocks()` | 종목 스크리닝 |
| `rebalance()` | 포트폴리오 리밸런싱 |
| `sell_losers()` | 손실 종목 매도 |
| `run_backtest()` | 백테스트 실행 |
| `calculate_performance()` | 성과 분석 |

---

## 커스터마이징

### 1. 투자 비율 변경

```python
backtest = KoreaStockBacktest(
    investment_ratio=0.8  # 80% 투자
)
```

### 2. 보유 종목 수 변경

```python
backtest = KoreaStockBacktest(
    num_stocks=30  # 30개 종목
)
```

### 3. 리밸런싱 주기 변경

현재는 연 1회이지만, 코드를 수정하여 분기별, 월별로 변경 가능합니다.

```python
# run_backtest() 함수에서 rebalance_dates 생성 부분 수정
# 예: 분기별
rebalance_dates = pd.date_range(start, end, freq='Q').strftime('%Y-%m-%d').tolist()
```

### 4. 스크리닝 조건 변경

```python
def screen_stocks(self, df):
    # 시가총액 하위 10%로 변경
    market_cap_10 = df['market_cap'].quantile(0.10)
    df = df[df['market_cap'] <= market_cap_10]
    
    # 부채비율 < 30%로 변경
    df = df[df['debt_ratio'] < 30]
```

---

## 주의사항

### 1. 데이터 한계

- **ROE 기반 수익성**: pykrx가 제공하는 EPS/BPS로 계산 (그린블라트 원형의 우수 대체지표)
- **OpenDART**: 추가 설정 시 매출총이익/총자산/부채비율 사용 가능
- **F-SCORE**: 미구현

→ 실제 투자를 위해서는 OpenDartReader 등을 활용하여 정확한 재무제표 데이터를 수집해야 합니다.

### 2. 생존편향

- 상장폐지 종목은 백테스트에서 제외됨
- 실제 수익률은 더 낮을 수 있음

### 3. 거래비용

- 기본 거래비용 0.2% 반영 (매수/매도 모두)
- 슬리피지/호가 충격은 미반영

### 4. 슬리피지

- 매수/매도 시 시장 충격 미반영
- 특히 소형주의 경우 주의 필요

---

## 개선 방안

### 1. 정확한 재무제표 데이터 사용

```python
from OpenDartReader import OpenDartReader

api_key = 'your_api_key'
dart = OpenDartReader(api_key)

# 재무제표 조회
fs = dart.finstate('005930', 2023)  # 삼성전자
```

### 2. GP/A 지표 추가

```python
def calculate_gp_a(self, ticker, date):
    """매출총이익/총자산 계산"""
    # DART API에서 재무제표 조회
    # 매출총이익 = 매출액 - 매출원가
    # GP/A = 매출총이익 / 총자산
```

### 3. F-SCORE 구현

```python
def calculate_fscore(self, fundamental_data):
    """피오트로스키 F-SCORE 계산"""
    # 9개 항목 평가:
    # - 수익성(4): ROA, CFO, △ROA, Accruals
    # - 레버리지/유동성(3): △부채비율, △유동비율, 신주발행
    # - 운영효율성(2): △매출총이익률, △자산회전율
```

### 4. 추세 추종 전략 추가

```python
def add_trend_filter(self):
    """추세 필터 추가로 MDD 감소"""
    # 이동평균선 활용
    # 시장 상황에 따른 투자 비율 조정
```

---

## 참고 자료

1. **책**: 『현명한 퀀트 주식투자』
2. **원본 전략**: 그린블라트 마법공식
3. **데이터**: 
   - [FinanceDataReader 문서](https://github.com/FinanceData/FinanceDataReader)
   - [pykrx 문서](https://github.com/sharebook-kr/pykrx)
   - [OpenDartReader 문서](https://github.com/FinanceData/OpenDartReader)

---

## 라이선스

MIT License

---

## 문의

백테스트 결과나 전략에 대한 질문이 있으시면 이슈를 등록해주세요.

**면책조항**: 이 코드는 교육 목적으로 제공됩니다. 실제 투자에 사용하기 전에 충분한 검증이 필요하며, 투자 손실에 대한 책임은 투자자 본인에게 있습니다.
