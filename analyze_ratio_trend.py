import pandas as pd
import numpy as np

RESULTS_FILE = r"c:\Users\HP\nse_tools\backtest_optimizer_results_NIFTY_v5.csv"

def analyze_ratio_trend():
    df = pd.read_csv(RESULTS_FILE)
    
    # Filter for the best strategy found in summary: Offset 200, SL 1.5
    best_df = df[(df['offset'] == 200) & (df['combo_id'] == '200_1.5')].copy()
    
    if best_df.empty:
        print("Error: Could not find results for Offset 200 / SL 1.5")
        return

    print(f"\nAnalysis for Ratio Iron Condor (1:2:2:1) | Offset 200 | SL 1.5")
    print(f"Total Sample Size: {len(best_df)} trades")

    # 1. Basic Correlations
    vix_corr = best_df['pnl_rupees'].corr(best_df['vix'])
    trend_corr = best_df['pnl_rupees'].corr(best_df['trend_diff'])
    
    print(f"Correlation with VIX: {vix_corr:.4f}")
    print(f"Correlation with Trend (EMA20 - Spot): {trend_corr:.4f}")

    # 2. Trend Regime Analysis
    # Let's bucket the trend_diff (EMA20 - Spot)
    # Positive trend_diff: Spot is BELOW EMA20 (Bearish)
    # Negative trend_diff: Spot is ABOVE EMA20 (Bullish)

    def trend_regime(diff):
        if diff > 100: return "Strong Bearish (Spot < EMA-100)"
        elif diff > 0: return "Weak Bearish (Spot < EMA)"
        elif diff > -100: return "Weak Bullish (Spot > EMA)"
        else: return "Strong Bullish (Spot > EMA+100)"
        
    best_df['regime'] = best_df['trend_diff'].apply(trend_regime)
    summary = best_df.groupby('regime')['pnl_rupees'].agg(['count', 'mean', 'sum']).round(2)
    win_percentages = best_df.groupby('regime').apply(lambda x: (x['pnl_rupees'] > 0).mean() * 100).round(2)
    summary['Win%'] = win_percentages
    
    print("\nPerformance by Trend Regime (Spot vs Daily 20-EMA):")
    print(summary.sort_values('sum', ascending=False))

    # 3. VIX Analysis (Already know it's mid-vol from earlier, but let's re-verify for Ratio Spreads)
    def vix_regime(v):
        if v < 14: return "Low Vol (<14)"
        elif v < 18: return "Mid Vol (14-18)"
        else: return "High Vol (>18)"
    
    best_df['vix_regime'] = best_df['vix'].apply(vix_regime)
    vix_summary = best_df.groupby('vix_regime')['pnl_rupees'].agg(['count', 'mean', 'sum']).round(2)
    print("\nPerformance by VIX Regime:")
    print(vix_summary.sort_values('sum', ascending=False))

if __name__ == "__main__":
    analyze_ratio_trend()
