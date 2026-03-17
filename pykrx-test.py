from dotenv import load_dotenv
import os

# 먼저 .env를 로드하고 pykrx 세션 패치를 활성화합니다.
load_dotenv()
from pykrx_session import enable_pykrx_session
enable_pykrx_session()

from pykrx import stock
from pykrx.website.krx.etx.ticker import EtxTicker
import pandas as pd

date = os.getenv("PYKRX_TEST_DATE", "20260315")

# 전체(또는 특정 마켓)의 티커 리스트
all_tickers = stock.get_market_ticker_list(date, market="ALL")
print(f"전체 티커 수: {len(all_tickers)}")

# ETF 전용 티커 리스트 (방어적 처리)
etf_tickers = []
try:
	etf_tickers = stock.get_etf_ticker_list(date)
	print(f"ETF 티커 수: {len(etf_tickers)}")
except Exception as e:
	print("pykrx.get_etf_ticker_list 실패:", e)
	etx = EtxTicker()
	print("EtxTicker.df.empty:", etx.df.empty)
	print("EtxTicker.columns:", etx.df.columns.tolist())

# 폴백: 로컬 CSV가 있으면 사용
if not etf_tickers:
	try:
		df_etf_local = pd.read_csv("results/krx_etf_list_naver.csv", dtype=str)
		# CSV 컬럼에 'ticker'가 없으면 첫 번째 컬럼을 사용
		if 'ticker' in df_etf_local.columns:
			etf_tickers = df_etf_local['ticker'].dropna().astype(str).tolist()
		else:
			etf_tickers = df_etf_local.iloc[:, 0].dropna().astype(str).tolist()
		print(f"로컬 CSV에서 불러온 ETF 수: {len(etf_tickers)}")
	except Exception as e:
		print("로컬 CSV 폴백 실패:", e)

# 포함 여부 확인
etf_in_all = set(all_tickers) & set(etf_tickers)
print(f"전체 리스트에 포함된 ETF 수: {len(etf_in_all)}")
# 각 티커가 ETF인지 플래그 추가
is_etf = {t: (t in etf_tickers) for t in all_tickers}