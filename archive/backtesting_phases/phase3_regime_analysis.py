"""
Phase 3: Regime Segmentation & Strategy Selection
Combines the outputs of Phase 2 logic to map the optimal strategy to distinct market regimes.
"""
import pandas as pd
import numpy as np

def categorize_vix(vix):
    if pd.isna(vix): return 'Unknown'
    if vix < 13: return 'Low (<13)'
    if vix < 18: return 'Mid (13-18)'
    return 'High (>18)'

def categorize_trend(trend_diff):
    if pd.isna(trend_diff): return 'Unknown'
    if trend_diff > 100: return 'Strong Bearish'
    if trend_diff > 0: return 'Weak Bearish'
    if trend_diff > -100: return 'Weak Bullish'
    return 'Strong Bullish'

def identify_regimes():
    print("=" * 60)
    print("  Phase 3: Regime Segmentation & Kill Zones")
    print("=" * 60)
    
    # Load all results
    dfs = []
    try: dfs.append(pd.read_csv("phase2_catA_NIFTY.csv"))
    except: pass
    try: dfs.append(pd.read_csv("phase2_catB_NIFTY.csv"))
    except: pass
    try: dfs.append(pd.read_csv("phase2_catCD_NIFTY.csv"))
    except: pass
    
    if not dfs:
        print("No Phase 2 results found.")
        return
        
    df = pd.concat(dfs, ignore_index=True)
    
    # Apply regime tags
    df['vix_regime'] = df['vix'].apply(categorize_vix)
    df['trend_regime'] = df['trend_diff'].apply(categorize_trend)
    df['regime_key'] = df['vix_regime'] + " | " + df['trend_regime']
    
    df['pnl_rupees'] = df['pnl_rupees'].astype(float)
    
    # 1. Overall Best Strategy Parameters
    # To prevent noise, we only want the overall best parameter combo for each strategy
    best_combos = {}
    for strat in df['strategy'].unique():
        sdf = df[df['strategy'] == strat]
        summary = sdf.groupby('combo_id').agg(
            trades=('pnl_rupees', 'count'),
            total_pnl=('pnl_rupees', 'sum')
        ).reset_index()
        best_combo = summary.loc[summary['total_pnl'].idxmax()]['combo_id']
        best_combos[strat] = best_combo
        
    print("\nSelected Optimal Parameters per Strategy:")
    for k, v in best_combos.items():
        print(f"  {k.ljust(15)}: {v}")
        
    # Filter df down to only use the optimal parameter combos
    df_opt = df[df['combo_id'].isin(best_combos.values())].copy()
    
    # 2. Strategy Performance per Regime
    results = []
    regimes = sorted(df_opt['regime_key'].unique())
    for regime in regimes:
        if 'Unknown' in regime: continue
        rdf = df_opt[df_opt['regime_key'] == regime]
        
        # Analyze each strategy in this regime
        strat_stats = []
        for strat in rdf['strategy'].unique():
            sdf = rdf[rdf['strategy'] == strat]
            trades = len(sdf)
            if trades < 5: continue # Ignore low sample sizes
            
            win_rate = (sdf['pnl_rupees'] > 0).mean() * 100
            avg_pnl = sdf['pnl_rupees'].mean()
            total_pnl = sdf['pnl_rupees'].sum()
            
            strat_stats.append({
                'strategy': strat,
                'trades': trades,
                'win_rate': round(win_rate, 1),
                'avg_pnl': round(avg_pnl, 1),
                'total_pnl': round(total_pnl, 1)
            })
            
        if not strat_stats: continue
        
        # Sort by expected EV (average pnl)
        strat_stats.sort(key=lambda x: x['avg_pnl'], reverse=True)
        
        # Best strategy for regime
        best = strat_stats[0]
        
        results.append({
            'Regime': regime,
            'Best Strategy': best['strategy'],
            'Expected Avg P&L': best['avg_pnl'],
            'Win Rate': f"{best['win_rate']}%",
            'Sample Size': best['trades']
        })
        
    print("\n" + "=" * 80)
    print("STRATEGY SELECTION MATRIX (Best Strategy Per Regime)")
    print("=" * 80)
    matrix_df = pd.DataFrame(results)
    if not matrix_df.empty:
        print(matrix_df.to_string(index=False))
        matrix_df.to_csv("phase3_strategy_matrix.csv", index=False)
    
    # 3. Identify Kill Zones
    print("\n" + "=" * 80)
    print("KILL ZONES (Regimes where no strategy averages > ₹300)")
    print("=" * 80)
    kill_zones = []
    for regime in regimes:
        if 'Unknown' in regime: continue
        rdf = df_opt[df_opt['regime_key'] == regime]
        best_pnl = -99999
        for strat in rdf['strategy'].unique():
            sdf = rdf[rdf['strategy'] == strat]
            if len(sdf) >= 5:
                best_pnl = max(best_pnl, sdf['pnl_rupees'].mean())
        if best_pnl < 300 and best_pnl != -99999: # 300 is our threshold to bother trading
            kill_zones.append({'Regime': regime, 'Best Possible Outcome (Avg P&L)': round(best_pnl, 1)})
            
    if kill_zones:
        print(pd.DataFrame(kill_zones).to_string(index=False))
    else:
        print("No Kill Zones identified (at least one strategy works in every tested regime).")

if __name__ == "__main__":
    identify_regimes()
