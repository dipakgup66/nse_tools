import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

# Configuration
RESULTS_FILE = r"c:\Users\HP\nse_tools\backtest_optimizer_results_NIFTY_v4.csv"
PLOT_DIR = r"c:\Users\HP\nse_tools\plots"

if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

def generate_visual_report():
    # 1. Load results
    df = pd.read_csv(RESULTS_FILE)
    
    # 2. Filter for the Best Baseline Strategy (Straddle, 09:30, 70-pt SL)
    # In V4, sl_val=0.0 means the 70pt baseline.
    strategy_df = df[
        (df['offset'] == 0) & 
        (df['sl_type'] == 'vix_points') & 
        (df['sl_val'] == 0.0) & 
        (df['entry_time'] == '09:30:00')
    ].copy()
    
    if strategy_df.empty:
        # Fallback to points strategy if sl_type was directly 'points' in an earlier run
        strategy_df = df[
            (df['offset'] == 0) & 
            (df['sl_type'] == 'points') & 
            (df['sl_val'] == 70.0) & 
            (df['entry_time'] == '09:30:00')
        ].copy()

    if strategy_df.empty:
        print("Error: Strategy not found in the results file.")
        return

    # 3. Time Series Processing
    strategy_df['date'] = pd.to_datetime(strategy_df['date'])
    strategy_df = strategy_df.sort_values('date')
    strategy_df['cumulative_pnl'] = strategy_df['pnl_rupees'].cumsum()
    
    # Calculate Drawdown
    strategy_df['peak'] = strategy_df['cumulative_pnl'].cummax()
    strategy_df['drawdown'] = strategy_df['cumulative_pnl'] - strategy_df['peak']
    
    # 4. Generate Equity Curve & Drawdown Plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [3, 1]})
    
    # Equity Curve
    ax1.plot(strategy_df['date'], strategy_df['cumulative_pnl'], color='blue', linewidth=2)
    ax1.set_title("Equity Curve: NIFTY Straddle (09:30 AM / 70-pt SL)", fontsize=14)
    ax1.set_ylabel("Cumulative P&L (₹)", fontsize=12)
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.fill_between(strategy_df['date'], strategy_df['cumulative_pnl'], color='blue', alpha=0.1)
    
    # Drawdown
    ax2.fill_between(strategy_df['date'], strategy_df['drawdown'], color='red', alpha=0.3)
    ax2.plot(strategy_df['date'], strategy_df['drawdown'], color='red', linewidth=1)
    ax2.set_title("Drawdown", fontsize=12)
    ax2.set_ylabel("Drawdown (₹)", fontsize=12)
    ax2.set_xlabel("Date", fontsize=12)
    ax2.grid(True, linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "nifty_equity_curve.png"), dpi=150)
    plt.close()
    
    # 5. Generate Monthly Heatmap
    strategy_df['year'] = strategy_df['date'].dt.year
    strategy_df['month'] = strategy_df['date'].dt.month
    
    monthly_pnl = strategy_df.groupby(['year', 'month'])['pnl_rupees'].sum().unstack()
    
    plt.figure(figsize=(12, 6))
    sns.heatmap(monthly_pnl, annot=True, fmt=".0f", cmap="RdYlGn", center=0, cbar_kws={'label': 'P&L (₹)'})
    plt.title("Monthly P&L Heatmap", fontsize=14)
    plt.xlabel("Month", fontsize=12)
    plt.ylabel("Year", fontsize=12)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "nifty_monthly_heatmap.png"), dpi=150)
    plt.close()
    
    print(f"Visual Reports generated in: {PLOT_DIR}")
    print(f"Max Drawdown: ₹{strategy_df['drawdown'].min():.2f}")
    print(f"Total Profit: ₹{strategy_df['cumulative_pnl'].iloc[-1]:.2f}")

if __name__ == "__main__":
    generate_visual_report()
