"""
Phase 5 & 6: Walk-Forward Validation and Portfolio Construction
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def run_portfolio_sim():
    print("="*60)
    print("  Phase 5 & 6: Walk-Forward Validation & Portfolio SIM")
    print("="*60)
    
    # 1. Load Data
    dfs = []
    for f in ["phase2_catA_NIFTY.csv", "phase2_catB_NIFTY.csv", "phase2_catCD_NIFTY.csv"]:
        try: dfs.append(pd.read_csv(f))
        except: pass
    all_trades = pd.concat(dfs, ignore_index=True)
    all_trades['pnl_rupees'] = all_trades['pnl_rupees'].astype(float)
    all_trades['date'] = pd.to_datetime(all_trades['date'])

    # 2. Optimal Combo IDs that powers our Matrix (Determined in Phase 3)
    best_combos = {
        'straddle': 'straddle_0_0_points_70_09:30:00',
        'strangle': 'strangle_100_0_leg_multiplier_1.4_09:20:00',
        'iron_condor': 'iron_condor_100_100_points_70_09:30:00',
        'iron_butterfly': 'iron_butterfly_0_200_points_70_09:30:00',
        'vwap': 'vwap_30_3.0',
        'orb': 'orb_60_1.5_1.0',
        'ema_cross': 'ema_9_21_50_2.0',
        'trend_swing': 'swing_2_0.3_1.5',
        'gamma_blast': 'gamma_13:00:00_3.0'
    }
    
    # The Matrix Strategy Selection
    # High: ema, orb
    # Low: vwap, orb, ema, straddle
    # Mid: straddle, gamma, orb, vwap
    selection_matrix = {
        'High (>18) | Strong Bearish': 'ema_cross',
        'High (>18) | Strong Bullish': 'orb',
        'Low (<13) | Strong Bearish': 'vwap',
        'Low (<13) | Strong Bullish': 'orb',
        'Low (<13) | Weak Bearish': 'ema_cross',
        'Low (<13) | Weak Bullish': 'straddle',
        'Mid (13-18) | Strong Bearish': 'straddle',
        'Mid (13-18) | Strong Bullish': 'gamma_blast',
        'Mid (13-18) | Weak Bearish': 'orb',
        'Mid (13-18) | Weak Bullish': 'vwap'
    }
    
    # Helper to apply regimes globally
    def get_v_regime(v):
        if pd.isna(v): return 'Unknown'
        if v < 13: return 'Low (<13)'
        elif v < 18: return 'Mid (13-18)'
        return 'High (>18)'
        
    def get_t_regime(t):
        if pd.isna(t): return 'Unknown'
        if t > 100: return 'Strong Bearish'
        if t > 0: return 'Weak Bearish'
        if t > -100: return 'Weak Bullish'
        return 'Strong Bullish'
        
    all_trades['v_reg'] = all_trades['vix'].apply(get_v_regime)
    all_trades['t_reg'] = all_trades['trend_diff'].apply(get_t_regime)
    all_trades['regime'] = all_trades['v_reg'] + " | " + all_trades['t_reg']
    
    # 3. Simulate Day by Day
    portfolio_daily = []
    
    # Get all unique dates where we have strategy data
    dates = sorted(all_trades['date'].unique())
    
    for d in dates:
        day_trades = all_trades[all_trades['date'] == d]
        if day_trades.empty: continue
        
        # Determine regime for the day
        day_vix = day_trades['vix'].iloc[0]
        day_trend = day_trades['trend_diff'].iloc[0]
        regime = get_v_regime(day_vix) + " | " + get_t_regime(day_trend)
        
        if regime in selection_matrix:
            chosen_strat = selection_matrix[regime]
            combo = best_combos[chosen_strat]
            match = day_trades[day_trades['combo_id'] == combo]
            if not match.empty:
                # We add the P&L from this trade (we assume 1 active trade per day based on matrix)
                pnl = match['pnl_rupees'].sum() # sum in case of multiple but typically 1
                portfolio_daily.append({'date': d, 'regime': regime, 'strategy': chosen_strat, 'pnl': pnl})
                
    pdf = pd.DataFrame(portfolio_daily)
    pdf = pdf.sort_values('date').reset_index(drop=True)
    pdf['cum_pnl'] = pdf['pnl'].cumsum()
    
    # High Water Mark & Drawdown
    pdf['hwm'] = pdf['cum_pnl'].cummax()
    pdf['drawdown'] = pdf['cum_pnl'] - pdf['hwm']
    
    # 4. Walk-Forward Validation
    # In-sample: Jan-Sep 2025. Wait, our data includes Jan 2022 to early 2026. Data coverage varies.
    # We will use all data up to 2025-09-30 as IS, and > 2025-09-30 as OOS.
    cutoff = pd.to_datetime('2025-09-30')
    is_df = pdf[pdf['date'] <= cutoff]
    oos_df = pdf[pdf['date'] > cutoff]
    
    # Metrics Calc
    def calc_metrics(df_sub):
        if df_sub.empty: return 0, 0, 0, 0, 0
        total_profit = df_sub['pnl'].sum()
        win_rate = (df_sub['pnl'] > 0).mean() * 100
        days = len(df_sub)
        max_dd = df_sub['drawdown'].min()
        
        if df_sub['pnl'].std() == 0: sharpe = 0
        else: sharpe = (df_sub['pnl'].mean() / df_sub['pnl'].std()) * np.sqrt(252)
        
        return total_profit, win_rate, days, max_dd, sharpe
        
    is_tot, is_wr, is_d, is_mdd, is_sharpe = calc_metrics(is_df)
    oos_tot, oos_wr, oos_d, oos_mdd, oos_sharpe = calc_metrics(oos_df)
    
    print(f"\n--- Walk Forward Validation ---")
    print(f"{'Metric'.ljust(15)} | {'In-Sample (Train)'.ljust(20)} | {'Out-of-Sample (Test)'.ljust(20)}")
    print("-" * 65)
    print(f"{'Time Period'.ljust(15)} | {'< Oct 2025'.ljust(20)} | {'>= Oct 2025'.ljust(20)}")
    print(f"{'Trading Days'.ljust(15)} | {str(is_d).ljust(20)} | {str(oos_d).ljust(20)}")
    print(f"{'Total P&L'.ljust(15)} | {f'Rs. {is_tot:,.0f}'.ljust(20)} | {f'Rs. {oos_tot:,.0f}'.ljust(20)}")
    print(f"{'Win Rate'.ljust(15)} | {f'{is_wr:.1f}%'.ljust(20)} | {f'{oos_wr:.1f}%'.ljust(20)}")
    print(f"{'Max Drawdown'.ljust(15)} | {f'Rs. {is_mdd:,.0f}'.ljust(20)} | {f'Rs. {oos_mdd:,.0f}'.ljust(20)}")
    print(f"{'Sharpe Ratio'.ljust(15)} | {f'{is_sharpe:.2f}'.ljust(20)} | {f'{oos_sharpe:.2f}'.ljust(20)}")
    
    # Degration
    # We can compare Expected P&L per day
    is_ev = is_tot / is_d if is_d else 0
    oos_ev = oos_tot / oos_d if oos_d else 0
    degradation = ((is_ev - oos_ev) / is_ev * 100) if is_ev > 0 else 0
    print(f"\nOut-of-Sample Degradation: {degradation:.1f}% (Health threshold < 30%)")

    # 5. Full Portfolio Metrics
    tot, wr, d, mdd, sharpe = calc_metrics(pdf)
    calmar = tot / abs(mdd) if mdd < 0 else 999
    
    print("\n--- Unified Portfolio Performance ---")
    print(f"Total Combined P&L : Rs. {tot:,.0f}")
    print(f"Overall Win Rate   : {wr:.1f}%")
    print(f"Annualized Sharpe  : {sharpe:.2f}")
    print(f"Max Drawdown       : Rs. {mdd:,.0f}")
    print(f"Calmar Ratio       : {calmar:.2f}")
    
    # Generate Charts
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [3, 1]})
    
    ax1.plot(pdf['date'], pdf['cum_pnl'], color='#00ff9d', linewidth=2)
    ax1.set_title('Unified Portfolio Equity Curve (Regime-Switching)', fontsize=14, pad=15)
    ax1.set_ylabel('Cumulative P&L (Rs.)')
    ax1.grid(True, alpha=0.2)
    ax1.axvline(x=cutoff, color='white', linestyle='--', alpha=0.5, label='Out of Sample Start')
    ax1.legend()
    
    ax2.fill_between(pdf['date'], pdf['drawdown'], 0, color='#ff3366', alpha=0.5)
    ax2.plot(pdf['date'], pdf['drawdown'], color='#ff3366', linewidth=1)
    ax2.set_title('Drawdown Profile', fontsize=12)
    ax2.set_ylabel('Drawdown (Rs.)')
    ax2.grid(True, alpha=0.2)
    
    plt.tight_layout()
    plt.savefig('portfolio_equity_curve.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("\nCharts generated and saved as portfolio_equity_curve.png")

if __name__ == "__main__":
    run_portfolio_sim()
