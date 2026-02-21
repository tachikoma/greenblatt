"""
백테스트 결과 시각화

사용법:
uv run visualize_backtest.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
from datetime import datetime

# 한글 폰트 설정 (Mac: AppleGothic, Windows: Malgun Gothic, Linux: NanumGothic)
try:
    plt.rcParams['font.family'] = 'AppleGothic'
except:
    try:
        plt.rcParams['font.family'] = 'Malgun Gothic'
    except:
        try:
            plt.rcParams['font.family'] = 'NanumGothic'
        except:
            print("한글 폰트를 찾을 수 없습니다. 기본 폰트를 사용합니다.")

plt.rcParams['axes.unicode_minus'] = False  # 마이너스 기호 깨짐 방지


def plot_portfolio_value(results, save_path='results/portfolio_value.png'):
    """포트폴리오 가치 변화 그래프"""
    
    df = results['portfolio_df']
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 포트폴리오 가치 선 그래프
    ax.plot(df['date'], df['portfolio_value'], 
            linewidth=2.5, color='#2E86AB', label='포트폴리오 가치')
    
    # 초기 투자금 기준선
    ax.axhline(y=results['initial_capital'], 
               color='red', linestyle='--', alpha=0.5, label='초기 투자금')
    
    # 그리드
    ax.grid(True, alpha=0.3)
    
    # 레이블
    ax.set_xlabel('날짜', fontsize=12, fontweight='bold')
    ax.set_ylabel('포트폴리오 가치 (원)', fontsize=12, fontweight='bold')
    ax.set_title('그린블라트 전략 - 포트폴리오 가치 변화 (실행일/영업일 기준)', 
                fontsize=16, fontweight='bold', pad=20)
    
    # 범례
    ax.legend(loc='upper left', fontsize=11)
    
    # Y축 포맷 (천 단위 구분)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    # 여백 조정
    plt.tight_layout()
    
    # 저장
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"그래프 저장: {save_path}")
    
    plt.close()


def plot_returns(results, save_path='results/returns.png'):
    """수익률 변화 그래프"""
    
    df = results['portfolio_df']
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 수익률 선 그래프
    returns_pct = df['return'] * 100
    ax.plot(df['date'], returns_pct, 
            linewidth=2.5, color='#A23B72', label='누적 수익률')
    
    # 0% 기준선
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # 양수/음수 영역 색칠
    ax.fill_between(df['date'], returns_pct, 0, 
                     where=(returns_pct >= 0), 
                     alpha=0.3, color='green', label='수익 구간')
    ax.fill_between(df['date'], returns_pct, 0, 
                     where=(returns_pct < 0), 
                     alpha=0.3, color='red', label='손실 구간')
    
    # 그리드
    ax.grid(True, alpha=0.3)
    
    # 레이블
    ax.set_xlabel('날짜', fontsize=12, fontweight='bold')
    ax.set_ylabel('누적 수익률 (%)', fontsize=12, fontweight='bold')
    ax.set_title('그린블라트 전략 - 누적 수익률 변화 (실행일/영업일 기준)', 
                fontsize=16, fontweight='bold', pad=20)
    
    # 범례
    ax.legend(loc='upper left', fontsize=11)
    
    # 여백 조정
    plt.tight_layout()
    
    # 저장
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"그래프 저장: {save_path}")
    
    plt.close()


def plot_drawdown(results, save_path='results/drawdown.png'):
    """낙폭(Drawdown) 그래프"""
    
    df = results['portfolio_df']
    
    # Drawdown 계산
    cummax = df['portfolio_value'].cummax()
    drawdown = (df['portfolio_value'] - cummax) / cummax * 100
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Drawdown 영역 그래프
    ax.fill_between(df['date'], drawdown, 0, 
                     alpha=0.5, color='#C73E1D', label='낙폭(Drawdown)')
    ax.plot(df['date'], drawdown, 
            linewidth=1.5, color='#8B0000', alpha=0.8)
    
    # 0% 기준선
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # MDD 표시
    mdd_idx = drawdown.idxmin()
    mdd_date = df.loc[mdd_idx, 'date']
    mdd_value = drawdown.min()
    
    ax.scatter([mdd_date], [mdd_value], 
              color='red', s=200, zorder=5, marker='v')
    ax.annotate(f'MDD: {mdd_value:.2f}%', 
               xy=(mdd_date, mdd_value),
               xytext=(10, -30), textcoords='offset points',
               fontsize=12, fontweight='bold',
               bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.7),
               arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0', color='red'))
    
    # 그리드
    ax.grid(True, alpha=0.3)
    
    # 레이블
    ax.set_xlabel('날짜', fontsize=12, fontweight='bold')
    ax.set_ylabel('낙폭 (%)', fontsize=12, fontweight='bold')
    ax.set_title('그린블라트 전략 - 낙폭(Drawdown) 분석 (실행일/영업일 기준)', 
                fontsize=16, fontweight='bold', pad=20)
    
    # 범례
    ax.legend(loc='lower left', fontsize=11)
    
    # 여백 조정
    plt.tight_layout()
    
    # 저장
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"그래프 저장: {save_path}")
    
    plt.close()


def plot_yearly_returns(results, save_path='results/yearly_returns.png'):
    """연도별 수익률 막대 그래프"""
    
    df = results['portfolio_df'].copy()
    df['year'] = df['date'].dt.year
    
    # 연도별 수익률 계산
    yearly_data = df.groupby('year').agg({
        'return': 'last'
    }).reset_index()
    
    # 전년 대비 수익률 계산
    yearly_data['year_return'] = yearly_data['return'].diff()
    yearly_data.loc[0, 'year_return'] = yearly_data.loc[0, 'return']
    yearly_data['year_return_pct'] = yearly_data['year_return'] * 100
    
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # 색상 설정 (양수: 초록, 음수: 빨강)
    colors = ['green' if x >= 0 else 'red' for x in yearly_data['year_return_pct']]
    
    # 막대 그래프
    bars = ax.bar(yearly_data['year'], yearly_data['year_return_pct'], 
                  color=colors, alpha=0.7, edgecolor='black')
    
    # 각 막대 위에 값 표시
    for i, (bar, value) in enumerate(zip(bars, yearly_data['year_return_pct'])):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{value:.1f}%',
               ha='center', va='bottom' if height >= 0 else 'top',
               fontsize=10, fontweight='bold')
    
    # 0% 기준선
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    
    # 그리드
    ax.grid(True, alpha=0.3, axis='y')
    
    # 레이블
    ax.set_xlabel('연도', fontsize=12, fontweight='bold')
    ax.set_ylabel('연간 수익률 (%)', fontsize=12, fontweight='bold')
    ax.set_title('그린블라트 전략 - 연도별 수익률 (실행일/영업일 기준)', 
                fontsize=16, fontweight='bold', pad=20)
    
    # X축 레이블 회전
    plt.xticks(rotation=45)
    
    # 여백 조정
    plt.tight_layout()
    
    # 저장
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"그래프 저장: {save_path}")
    
    plt.close()


def plot_composition(results, save_path='results/composition.png'):
    """자산 구성 비율 그래프 (현금 vs 주식)"""
    
    df = results['portfolio_df']
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 스택 영역 그래프
    ax.fill_between(df['date'], 0, df['cash'], 
                     alpha=0.6, color='#FFD700', label='현금')
    ax.fill_between(df['date'], df['cash'], df['portfolio_value'], 
                     alpha=0.6, color='#4169E1', label='주식')
    
    # 경계선
    ax.plot(df['date'], df['portfolio_value'], 
            linewidth=2, color='black', alpha=0.5, label='총 자산')
    
    # 그리드
    ax.grid(True, alpha=0.3)
    
    # 레이블
    ax.set_xlabel('날짜', fontsize=12, fontweight='bold')
    ax.set_ylabel('자산 (원)', fontsize=12, fontweight='bold')
    ax.set_title('그린블라트 전략 - 자산 구성 비율 (현금 vs 주식, 실행일/영업일 기준)', 
                fontsize=16, fontweight='bold', pad=20)
    
    # Y축 포맷
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    # 범례
    ax.legend(loc='upper left', fontsize=11)
    
    # 여백 조정
    plt.tight_layout()
    
    # 저장
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"그래프 저장: {save_path}")
    
    plt.close()


def plot_transaction_costs(results, save_path='results/transaction_costs.png'):
    """연도별 거래비용 그래프"""
    trades_df = results.get('trades_df')

    if trades_df is None or trades_df.empty or 'fee' not in trades_df.columns:
        print("거래비용 데이터가 없어 거래비용 그래프 생성을 건너뜁니다.")
        return

    trades = trades_df.copy()
    trades['date'] = pd.to_datetime(trades['date'])
    trades['year'] = trades['date'].dt.year

    yearly_fee = trades.groupby('year')['fee'].sum().reset_index()

    fig, ax = plt.subplots(figsize=(12, 7))

    bars = ax.bar(
        yearly_fee['year'], yearly_fee['fee'],
        color='#FF8C42', alpha=0.8, edgecolor='black'
    )

    for bar, value in zip(bars, yearly_fee['fee']):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2., height,
            f'{value:,.0f}원',
            ha='center', va='bottom', fontsize=9, fontweight='bold'
        )

    ax.grid(True, alpha=0.3, axis='y')
    ax.set_xlabel('연도', fontsize=12, fontweight='bold')
    ax.set_ylabel('연간 거래비용 (원)', fontsize=12, fontweight='bold')
    ax.set_title('그린블라트 전략 - 연도별 거래비용', fontsize=16, fontweight='bold', pad=20)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"그래프 저장: {save_path}")

    plt.close()


def plot_summary_dashboard(results, save_path='results/dashboard.png'):
    """종합 대시보드"""
    
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.3)
    
    df = results['portfolio_df']
    
    # 1. 포트폴리오 가치
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(df['date'], df['portfolio_value'], linewidth=2.5, color='#2E86AB')
    ax1.axhline(y=results['initial_capital'], color='red', linestyle='--', alpha=0.5)
    ax1.grid(True, alpha=0.3)
    ax1.set_title('포트폴리오 가치 변화 (실행일/영업일 기준)', fontsize=14, fontweight='bold')
    ax1.set_ylabel('가치 (원)', fontsize=11)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    # 2. 수익률
    ax2 = fig.add_subplot(gs[1, 0])
    returns_pct = df['return'] * 100
    ax2.plot(df['date'], returns_pct, linewidth=2, color='#A23B72')
    ax2.fill_between(df['date'], returns_pct, 0, where=(returns_pct >= 0), 
                     alpha=0.3, color='green')
    ax2.fill_between(df['date'], returns_pct, 0, where=(returns_pct < 0), 
                     alpha=0.3, color='red')
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax2.grid(True, alpha=0.3)
    ax2.set_title('누적 수익률 (실행일/영업일 기준)', fontsize=14, fontweight='bold')
    ax2.set_ylabel('수익률 (%)', fontsize=11)
    
    # 3. Drawdown
    ax3 = fig.add_subplot(gs[1, 1])
    cummax = df['portfolio_value'].cummax()
    drawdown = (df['portfolio_value'] - cummax) / cummax * 100
    ax3.fill_between(df['date'], drawdown, 0, alpha=0.5, color='#C73E1D')
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax3.grid(True, alpha=0.3)
    ax3.set_title('낙폭 (Drawdown, 실행일/영업일 기준)', fontsize=14, fontweight='bold')
    ax3.set_ylabel('낙폭 (%)', fontsize=11)
    
    # 4. 성과 지표 테이블
    ax4 = fig.add_subplot(gs[2, 0])
    ax4.axis('off')
    
    metrics = [
        ['지표', '값'],
        ['초기 자본', f"{int(round(results['initial_capital'])):,}원"],
        ['최종 자산', f"{results['final_value']:,.0f}원"],
        ['총 수익률', f"{results['total_return_pct']:.2f}%"],
        ['CAGR', f"{results['cagr_pct']:.2f}%"],
        ['MDD', f"{results['mdd_pct']:.2f}%"],
        ['승률', f"{results['win_rate_pct']:.2f}%"],
            ['샤프 비율', f"{results.get('sharpe_ratio', 0):.2f}"],
        ['총 거래비용', f"{results.get('total_fee', 0):,.0f}원"],
    ]
    
    table = ax4.table(
        cellText=metrics,
        cellLoc='left',
        colWidths=[0.4, 0.6],
        bbox=[0.0, -0.03, 1.0, 0.95]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.25)
    
    # 헤더 스타일
    for i in range(2):
        table[(0, i)].set_facecolor('#4169E1')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # 데이터 행 스타일
    for i in range(1, len(metrics)):
        for j in range(2):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#F0F0F0')
    
    ax4.set_title('성과 지표', fontsize=14, fontweight='bold', pad=2)
    
    # 5. 연도별 수익률
    ax5 = fig.add_subplot(gs[2, 1])
    df_copy = df.copy()
    df_copy['year'] = df_copy['date'].dt.year
    yearly_data = df_copy.groupby('year').agg({'return': 'last'}).reset_index()
    yearly_data['year_return'] = yearly_data['return'].diff()
    yearly_data.loc[0, 'year_return'] = yearly_data.loc[0, 'return']
    yearly_data['year_return_pct'] = yearly_data['year_return'] * 100
    
    colors = ['green' if x >= 0 else 'red' for x in yearly_data['year_return_pct']]
    bars = ax5.bar(yearly_data['year'], yearly_data['year_return_pct'], 
                   color=colors, alpha=0.7, edgecolor='black')
    
    for bar, value in zip(bars, yearly_data['year_return_pct']):
        height = bar.get_height()
        ax5.text(bar.get_x() + bar.get_width()/2., height,
                f'{value:.1f}%',
                ha='center', va='bottom' if height >= 0 else 'top',
                fontsize=9, fontweight='bold')
    
    ax5.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax5.grid(True, alpha=0.3, axis='y')
    ax5.set_title('연도별 수익률 (실행일/영업일 기준)', fontsize=14, fontweight='bold')
    ax5.set_ylabel('수익률 (%)', fontsize=11)
    plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
    
    # 전체 제목
    fig.suptitle('그린블라트 전략 백테스트 종합 대시보드 (실행일/영업일 기준)', 
                fontsize=18, fontweight='bold', y=0.98)
    
    # 저장
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"대시보드 저장: {save_path}")
    
    plt.close()


def main():
    """메인 함수 - CSV 파일에서 결과 로드 및 시각화"""

    print("\n백테스트 결과 시각화 시작...\n")

    try:
        # CSV 파일 로드
        portfolio_df = pd.read_csv('results/backtest_portfolio.csv')
        portfolio_df['date'] = pd.to_datetime(portfolio_df['date'])

        # 거래 내역 로드 (거래비용 계산용)
        trades_df = pd.read_csv('results/backtest_trades.csv')
        if 'fee' not in trades_df.columns:
            trades_df['fee'] = 0.0
        trades_df['fee'] = pd.to_numeric(trades_df['fee'], errors='coerce').fillna(0.0)

        # 기본 지표 계산
        portfolio_df = portfolio_df.sort_values('date').reset_index(drop=True)
        portfolio_df['period_return'] = portfolio_df['portfolio_value'].pct_change()

        initial_capital = int(round(portfolio_df['portfolio_value'].iloc[0] / (1 + portfolio_df['return'].iloc[0])))
        final_value = portfolio_df['portfolio_value'].iloc[-1]
        total_return_pct = portfolio_df['return'].iloc[-1] * 100

        # MDD
        cummax = portfolio_df['portfolio_value'].cummax()
        drawdown = (portfolio_df['portfolio_value'] - cummax) / cummax
        mdd_pct = drawdown.min() * 100

        # CAGR
        years = (portfolio_df['date'].iloc[-1] - portfolio_df['date'].iloc[0]).days / 365.25
        cagr_pct = ((final_value / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0

        # 승률
        winning = (portfolio_df['period_return'] > 0).sum()
        total_periods = len(portfolio_df[portfolio_df['period_return'].notna()])
        win_rate_pct = (winning / total_periods * 100) if total_periods > 0 else 0

        # 샤프 비율 (기간별 수익률 기반, 간단 연환산)
        pr = portfolio_df['period_return'].dropna()
        if pr.std() > 0 and len(pr) > 0:
            sharpe_ratio = pr.mean() / pr.std() * np.sqrt(len(pr))
        else:
            sharpe_ratio = 0.0

        results = {
            'portfolio_df': portfolio_df,
            'trades_df': trades_df,
            'initial_capital': initial_capital,
            'final_value': final_value,
            'total_return_pct': total_return_pct,
            'cagr_pct': cagr_pct,
            'mdd_pct': mdd_pct,
            'win_rate_pct': win_rate_pct,
            'sharpe_ratio': sharpe_ratio,
            'total_fee': trades_df['fee'].sum()
        }

        # 그래프 생성
        print("1. 포트폴리오 가치 변화 그래프 생성...")
        plot_portfolio_value(results)

        print("2. 수익률 변화 그래프 생성...")
        plot_returns(results)

        print("3. 낙폭(Drawdown) 그래프 생성...")
        plot_drawdown(results)

        print("4. 연도별 수익률 그래프 생성...")
        plot_yearly_returns(results)

        print("5. 자산 구성 비율 그래프 생성...")
        plot_composition(results)

        print("6. 연도별 거래비용 그래프 생성...")
        plot_transaction_costs(results)

        print("7. 종합 대시보드 생성...")
        plot_summary_dashboard(results)

        print("\n✅ 모든 그래프 생성 완료!")
        print("\n생성된 파일:")
        print("  - portfolio_value.png: 포트폴리오 가치 변화")
        print("  - returns.png: 수익률 변화")
        print("  - drawdown.png: 낙폭 분석")
        print("  - yearly_returns.png: 연도별 수익률")
        print("  - composition.png: 자산 구성")
        print("  - transaction_costs.png: 연도별 거래비용")
        print("  - dashboard.png: 종합 대시보드")

    except FileNotFoundError:
        print("❌ 오류: backtest_portfolio.csv 파일을 찾을 수 없습니다.")
        print("먼저 백테스트를 실행하세요: uv run greenblatt_korea_full_backtest.py")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()
