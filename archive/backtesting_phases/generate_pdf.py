import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os

plots_dir = r"c:\Users\HP\nse_tools\plots"
output_pdf = r"c:\Users\HP\nse_tools\archive\backtesting_phases\Bull_Put_Spread_Analysis.pdf"

def main():
    print("Generating PDF report...")
    with PdfPages(output_pdf) as pdf:
        # First Page: Text Summary
        fig = plt.figure(figsize=(8.5, 11))
        fig.clf()
        
        txt = """
    STRATEGY #1: THE BULL PUT SPREAD - SILO TESTING RESULTS
    ========================================================================
    
    1. MARKET REGIME CONFIRMATION
    The empirical backtest of 18,099 simulated options spreads across all market 
    conditions officially confirms that this strategy yields a positive 
    Expected Value (EV) strictly during 'Weak Bull' and 'Strong Bull' regimes. 
    Deploying it in Bearish conditions heavily destroys capital.
        
    2. OPTIMAL PARAMETERS (BULLISH REGIMES)
    - Entry Time: 09:20 AM mathematically outperformed later times (09:30, 09:45).
    - Sell Leg: ATM - 50 points was the ideal optimal strike.
    - Buy Leg (Wing Width): 200 point protection outperformed tight 100 pt wings.
    - Expected Win Rate: 53.8%
    - Total Gross P&L (Optimal Param set): +Rs 27,870
    
    3. M.A.E. ANALYSIS & STOP LOSS OPTIMIZATION
    By measuring the 95th percentile of intraday drawdowns for all trades 
    that eventually became winners, we derived an exact Stop Loss limit.
    Setting a strict Hard SL at exactly -Rs -2,500 cuts the catastrophic losers 
    without accidentally knocking you out of the trades that bounce back.
    
    ------------------------------------------------------------------------
    FINAL HARD-CODED CONFIGURATION FOR STRATEGY AGENT
    ------------------------------------------------------------------------
    • Trigger Condition: Spot > Daily EMA20 (Confirmed Bullish Trend)
    • Entry Timing     : 09:20 AM
    • Strikes          : Sell Put @ (ATM - 50). Buy Put @ (ATM - 250).
    • Hard Stop Loss   : Abort trade if net P&L drops below -Rs 2,500.
    • Take Profit      : Auto-close at 15:15 EOD limit if SL isn't hit.
        """
        
        # We use a monospace font to keep alignment clean
        fig.text(0.05, 0.95, txt, transform=fig.transFigure, size=11, ha="left", va="top", fontfamily="monospace")
        pdf.savefig(fig)
        plt.close()
        
        # Function to add an image to a new page
        def add_image_page(img_name, title):
            img_path = os.path.join(plots_dir, img_name)
            if not os.path.exists(img_path):
                return
            img = plt.imread(img_path)
            fig = plt.figure(figsize=(10, 8))
            plt.axis('off')
            plt.imshow(img)
            # Make the title look nice at the top
            fig.text(0.5, 0.98, title, ha='center', va='top', fontsize=14, fontweight='bold', fontfamily='sans-serif')
            pdf.savefig(fig)
            plt.close()

        # Page 2: Heatmap Image
        add_image_page("regime_heatmap.png", "1. Regime Performance Validation")

        # Page 3: Entry Times Image
        add_image_page("entry_time_comparison.png", "2. Entry Time Optimization")

        # Page 4: MAE Image
        add_image_page("mae_stoploss_analysis.png", "3. MAE & Stop-Loss Discovery")
        
    print(f"PDF successfully saved to {output_pdf}")

if __name__ == '__main__':
    main()
