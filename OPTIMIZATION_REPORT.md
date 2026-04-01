# 백테스트/선정 엔진 최적화 현황 (최신 코드 기준)

이 문서는 특정 날짜의 단발성 벤치마크 수치보다, 현재 소스에 구현된 최적화 메커니즘을 중심으로 유지합니다.

## 1. 현재 적용된 최적화

### 1) 2계층 캐시

- 메모리 캐시: 실행 중 재사용
- 디스크 캐시: 재실행 시 재사용
- 주요 대상
  - 펀더멘털/시총 (`results/cache/fundamentals/*`)
  - 모멘텀 (`results/cache/momentum_cache.json`)
  - 업종/티커 리스트 (`industry_cache.json`, `ticker_list_cache.json`)

### 2) 펀더멘털 저장 포맷 개선

- `parquet` 우선 저장/로드
- 실패 시 `csv` 폴백
- 파일 단위: `{date}_{market}`

### 3) 캐시 버전 검증 및 무효화

- `cache_meta.json`의 버전/핵심 파라미터를 확인
- 불일치 시 비호환 캐시 자동 정리

### 4) LRU 기반 메모리 캐시 제한

- 펀더멘털 메모리 캐시는 최대 엔트리 수 제한(`fundamental_cache_max_entries`)
- 오래된 항목부터 제거

### 5) 타이밍 로그

- 주요 구간별 `[TIME]` 로그 출력
- 병목 구간 확인 및 실험 비교에 활용 가능

---

## 2. 현재 코드에서 확인되는 기본값

- 리밸런싱 기본 주기: 3개월(`DEFAULT_REBALANCE_MONTHS`)
- 백테스트 기본 실행 파라미터(엔트리포인트 기준)
  - `investment_ratio=0.95`
  - `num_stocks=40`
  - `strategy_mode='mixed'`
  - `mixed_filter_profile='large_cap'`
  - `momentum_enabled=true`, `momentum_months=3`, `momentum_weight=0.60`

---

## 3. 병목 가능 지점 (운영 관점)

환경/기간/네트워크에 따라 달라지지만 일반적으로 다음 순서로 비용이 큽니다.

1. 외부 시세/펀더멘털 네트워크 I/O
2. 종목별 모멘텀 계산(캐시 미스 구간)
3. 대량 종목 처리 시 정렬/랭킹 연산

---

## 4. 재현 가능한 성능 측정 권장 절차

1. 캐시 초기화 후 1회 실행(Cold)
2. 동일 조건 재실행(Warm)
3. 두 실행의 `[TIME]` 로그 비교
4. 변경점(파라미터, 기간, 소스)을 실험 메모에 함께 기록

예시:

```bash
rm -rf results/cache
uv run greenblatt_korea_full_backtest.py
uv run greenblatt_korea_full_backtest.py
```

---

## 5. 향후 개선 후보

- 모멘텀 계산 구간의 병렬도/재시도 정책 튜닝
- 캐시 관측성 강화(히트율, 키 수, 파일 크기 요약 로그)
- 장기 실행 시 캐시 정리 정책(보존 기간/상한) 명시화
