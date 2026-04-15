import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def categorize_trend(pct):
    if pct < -2.0: return 'Strong Bear'
    if pct < 0.0:  return 'Weak Bear'
    if pct < 2.0:  return 'Weak Bull'
    return 'Strong Bull'

def categorize_vix(vix):
    if vix < 13: return 'Low VIX (<13)'
    if vix < 18: return 'Mid VIX (13-18)'
    return 'High VIX (>18)'

def main():
    print("Loading unfiltered backtest data...")
    try:
        df = pd.read_csv("silo_1_bull_put_unfiltered.csv")
    except FileNotFoundError:
        print("silo_1_bull_put_unfiltered.csv not found yet.")
        return
        
    print(f"Loaded {len(df)} total trades.")
    
    # Calculate basic features
    # Assume trading 1 lot of NIFTY = 75 qty
    df['gross_pnl'] = df['exit_pnl_pts'] * 75
    df['mae_rs'] = df['mae_pts'] * 75
    
    # Tag Regimes
    df['trend_regime'] = df['trend_ema_pct'].apply(categorize_trend)
    df['vix_regime'] = df['vix'].apply(categorize_vix)
    df['regime_combo'] = df['vix_regime'] + " | " + df['trend_regime']
    
    os.makedirs(r"c:\Users\HP\nse_tools\plots", exist_ok=True)
    
    # ---------------------------------------------------------
    # 1. Regime Performance Heatmap
    # ---------------------------------------------------------
    # First find the best parameters across all regimes so we aren't muddying the waters with terrible strikes
    # Instead, let's just chart the performance of the ATM / 100 wing as a baseline
    baseline = df[(df['offset'] == 0) & (df['wing'] == 100)]
    pivot_baseline = baseline.pivot_table(index='vix_regime', columns='trend_regime', values='gross_pnl', aggfunc='mean')
    
    # Reorder columns and rows for logical flow
    trend_order = ['Strong Bear', 'Weak Bear', 'Weak Bull', 'Strong Bull']
    vix_order = ['Low VIX (<13)', 'Mid VIX (13-18)', 'High VIX (>18)']
    pivot_baseline = pivot_baseline.reindex(index=vix_order, columns=[c for c in trend_order if c in pivot_baseline.columns])
    
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_baseline, annot=True, fmt=".0f", cmap="RdYlGn", center=0)
    plt.title("Expected PnL per Trade by Market Regime\n(Bull Put Spread: ATM, 100pt Wing)")
    plt.tight_layout()
    plt.savefig(r"c:\Users\HP\nse_tools\plots\regime_heatmap.png")
    plt.close()
    
    # ---------------------------------------------------------
    # 2. Optimal Parameter Search (In Bullish Regimes only)
    # ---------------------------------------------------------
    # We filter ONLY for days where we are in Weak Bull or Strong Bull
    bull_df = df[df['trend_regime'].isin(['Weak Bull', 'Strong Bull'])]
    
    # Group by the parameter combinations
    params = bull_df.groupby(['entry_time', 'offset', 'wing']).agg(
        trades=('gross_pnl', 'count'),
        win_rate=('gross_pnl', lambda x: (x > 0).mean() * 100),
        avg_pnl=('gross_pnl', 'mean'),
        total_pnl=('gross_pnl', 'sum'),
        avg_mae=('mae_rs', 'mean'),
        max_mae=('mae_rs', 'min')  # MAE is negative, so min is worst drawdown
    ).reset_index()
    
    params = params.sort_values('avg_pnl', ascending=False)
    
    print("\n" + "="*80)
    print("TOP 5 PERFORMING PARAMETERS (DURING BULLISH TRENDS)")
    print("="*80)
    print(params.head(5).to_string(index=False))
    
    # ---------------------------------------------------------
    # 3. Entry Time Analysis Graphic
    # ---------------------------------------------------------
    # Using the best strike params (from row 1), let's compare Entry Times
    best_offset = params.iloc[0]['offset']
    best_wing = params.iloc[0]['wing']
    best_strike_df = bull_df[(bull_df['offset'] == best_offset) & (bull_df['wing'] == best_wing)]
    
    plt.figure(figsize=(8, 5))
    sns.barplot(data=best_strike_df, x='entry_time', y='gross_pnl', ci=68)
    plt.title(f"Average PnL by Morning Entry Time\n(Bullish Regimes | Offset:{best_offset}, Wing:{best_wing})")
    plt.ylabel("Avg Expected PnL (Rs)")
    plt.tight_layout()
    plt.savefig(r"c:\Users\HP\nse_tools\plots\entry_time_comparison.png")
    plt.close()
    
    # ---------------------------------------------------------
    # 4. MAE (Pain) vs Final Outcome Scatter
    # ---------------------------------------------------------
    # Lets take the optimal combination to find out what our Stop Loss should be.
    opt_df = best_strike_df[best_strike_df['entry_time'] == params.iloc[0]['entry_time']]
    
    # Separate winners and losers
    winners = opt_df[opt_df['gross_pnl'] > 0]
    losers = opt_df[opt_df['gross_pnl'] <= 0]
    
    # 95th percentile MAE for winners (The deepest a winning trade went into the red)
    safe_stop_loss = winners['mae_rs'].quantile(0.05) 
    
    plt.figure(figsize=(10, 6))
    plt.scatter(winners['mae_rs'], winners['gross_pnl'], color='green', alpha=0.5, label='Winning Trades')
    plt.scatter(losers['mae_rs'], losers['gross_pnl'], color='red', alpha=0.5, label='Losing Trades')
    plt.axvline(x=safe_stop_loss, color='black', linestyle='--', label=f'Suggested SL (Rs {safe_stop_loss:,.0f})')
    
    plt.title(f"MAE (Intraday Drawdown) vs Final Exit PnL")
    plt.xlabel("Maximum Intraday Drawdown (Rs)")
    plt.ylabel("Final EOD P&L (Rs)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(r"c:\Users\HP\nse_tools\plots\mae_stoploss_analysis.png")
    plt.close()
    
    print("\n" + "="*80)
    print("M.A.E. ANALYSIS (Finding the Ideal Stop Loss)")
    print("="*80)
    print(f"If we look at ALL the trades that eventually won at EOD...")
    print(f"95% of those winning trades never dipped below: Rs {safe_stop_loss:,.0f} intraday.")
    print(f"Therefore, setting a strict Hard SL at exactly Rs {safe_stop_loss:,.0f} saves capital")
    print("on the catastrophic losers without accidentally knocking you out of the eventual winners.")

if __name__ == '__main__':
    main()
