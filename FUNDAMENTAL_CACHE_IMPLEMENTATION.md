# 펀더멘털/시총 캐시 구현 문서 (최신 코드 기준)

## 개요

현재 `stock_selector.py` 및 백테스트 래퍼는 펀더멘털/시총 조회에 대해
- 메모리 캐시
- 디스크 캐시
를 순차적으로 사용하며, 캐시 미스 시에만 외부 API를 호출합니다.

## 1. 캐시 계층

### 1) 메모리 캐시

- 키: `{date}|{market}`
- 타입: `OrderedDict` 기반 LRU
- 최대 개수: `fundamental_cache_max_entries` (기본 16)

### 2) 디스크 캐시

- 경로: `results/cache/fundamentals/`
- 파일명: `{date}_{market}.parquet` (우선)
- 폴백: parquet 실패 시 csv 저장/로드

### 3) 캐시 메타

- 파일: `results/cache/cache_meta.json`
- 목적: 버전/핵심 파라미터 호환성 검증

---

## 2. 캐시 조회 순서

1. 메모리 캐시 확인
2. 디스크 캐시 확인(parquet -> csv)
3. API 호출 후 캐시에 저장

요약 의사코드:

```python
if key in memory_cache:
    return memory_cache[key]
if file_exists_on_disk:
    frame = load_from_disk()
    memory_cache[key] = frame
    return frame
frame = fetch_from_api()
save_to_disk(frame)
memory_cache[key] = frame
return frame
```

---

## 3. 버전 무효화 정책

다음 조건이 달라지면 비호환 캐시로 판단해 정리합니다.

- `fundamental_cache_v`
- `momentum_cache_v`
- `strategy_mode`
- `momentum_months`
- `cache_format`
- `fundamental_source`

이 정책으로 구조 변경 후 구캐시 오염 가능성을 줄입니다.

---

## 4. 관련 환경변수

```bash
FUNDAMENTAL_SOURCE=auto
LIVE_FUNDAMENTAL_SOURCE=kiwoom
BACKTEST_FUNDAMENTAL_SOURCE=pykrx

KIWOOM_FUND_ENDPOINT=/api/dostk/stkinfo
KIWOOM_FUND_API_ID=ka10001
KIWOOM_FUND_CONCURRENCY=3
KIWOOM_FUND_MAX=0

KIWOOM_ALLOW_DATE_PROXY=false

KIWOOM_PREFILTER_ENABLED=true
KIWOOM_PREFILTER_TARGET=500
KIWOOM_PREFILTER_MIN_MCAP=50000000000
KIWOOM_PREFILTER_MIN_TRADING_VALUE=0

KIWOOM_STOCK_LIST_ENDPOINT=/api/dostk/stkinfo
KIWOOM_STOCK_LIST_API_ID=ka10099
KIWOOM_STOCK_LIST_MAX_PAGES=50
```

---

## 5. 운영 체크리스트

- 재현 실험 전 캐시 초기화 여부를 명시
- 소스 전환(`pykrx`/`kiwoom`) 시 캐시 버전 및 결과 차이 확인
- 대규모 기간 실험에서는 캐시 디렉터리 용량 점검

예시:

```bash
rm -rf results/cache
uv run greenblatt_korea_full_backtest.py
```

---

## 6. 비고

과거 문서에 있던 고정 실행시간/배수 개선 수치는
환경(네트워크, API 상태, 기간)에 따라 달라져 오해 소지가 있어 제거했습니다.
현재 문서는 구현 메커니즘과 운영 방법 중심으로 유지합니다.
