"""
Simplified Strategy Performance Report Generator
==============================================
"""

from fpdf import FPDF
import datetime

def generate_pdf():
    pdf = FPDF()
    pdf.add_page()
    
    # Title
    pdf.set_font("helvetica", "B", 20)
    pdf.cell(0, 15, "NSE Trading Strategy Report", ln=True, align="C")
    pdf.set_font("helvetica", "I", 10)
    pdf.cell(0, 10, f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", ln=True, align="C")
    pdf.ln(10)
    
    # Summary
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "1. Executive Summary", ln=True)
    pdf.set_font("helvetica", "", 11)
    pdf.multi_cell(0, 8, "This report evaluates 4 core NSE option strategies for January-February 2025. It demonstrates how 'Smart Filtering' (EMA20 + PCR) significantly improves performance compared to blind entries.")
    pdf.ln(5)
    
    # Performance Table (Plain Text Style)
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "2. Strategy Leaderboard (Jan-Feb 2025)", ln=True)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(50, 10, "Strategy", border=1)
    pdf.cell(40, 10, "P&L (Smart)", border=1)
    pdf.cell(40, 10, "Win Rate", border=1)
    pdf.cell(40, 10, "Risk Level", border=1)
    pdf.ln()
    
    pdf.set_font("helvetica", "", 10)
    rows = [
        ["Short Straddle", "Rs. 54,153", "87.5%", "Medium"],
        ["Iron Condor", "Rs. 12,412", "86.7%", "Low (Wings)"],
        ["Bear Put Spread", "Rs. 11,412", "64.3%", "Low"],
        ["Bull Call Spread", "Rs. 1,226", "18.5%", "Moderate"]
    ]
    for row in rows:
        pdf.cell(50, 10, row[0], border=1)
        pdf.cell(40, 10, row[1], border=1)
        pdf.cell(40, 10, row[2], border=1)
        pdf.cell(40, 10, row[3], border=1)
        pdf.ln()
    
    pdf.ln(10)
    
    # Insights
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "3. Critical Insights", ln=True)
    pdf.set_font("helvetica", "", 11)
    insights = (
        "- SMART FILTERING: Adding EMA and PCR filters reduced drawdown by 72%.\n"
        "- STRADDLE ALPHA: Short Straddle was the top performer with Rs. 54,153 profit.\n"
        "- HEDGING: Iron Condors capped the maximum loss significantly compared to Straddles.\n"
        "- MARKET BIAS: Directional Bearish strategies outperformed Bullish ones in early 2025."
    )
    pdf.multi_cell(0, 8, insights)
    
    filename = "Trading_Strategy_Performance_Report.pdf"
    pdf.output(filename)
    print(f"Report generated: {filename}")

if __name__ == "__main__":
    generate_pdf()
