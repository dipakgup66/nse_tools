import pandas as pd
import numpy as np

# Load the original router
try:
    df = pd.read_csv("data/Master_Regime_Router.csv")
except Exception as e:
    print("Could not load original data:", e)
    exit()

# Add Expectancy (Since we only have Avg_PnL, we assume Avg_PnL is the Expected Value per trade)
# Expectancy = Avg_PnL (Absolute currency edge per trade)
df['Expectancy_Rs'] = df['Avg_PnL']

# Calculate Confidence Score
# Formula: log10(Trade_Count + 1) * Win_Rate_Pct * (Expectancy_Rs if positive else 0)
# (Just a relative scoring system to penalize low samples or negative expectancy)
df['Confidence_Score'] = np.log10(df['Trade_Count'] + 1) * df['Win_Rate_Pct']

# Round columns
df['Confidence_Score'] = df['Confidence_Score'].round(2)
df['Expectancy_Rs'] = df['Expectancy_Rs'].round(2)

# Create a filtered version to show what would happen if we strictly dropped < 30 samples vs a softer limit
print(f"Total Rows original: {len(df)}")
print(f"Rows with >= 30 trades: {len(df[df['Trade_Count'] >= 30])}")
print(f"Rows with >= 15 trades: {len(df[df['Trade_Count'] >= 15])}")

# Let's clean the DataFrame by replacing 'Best_Strategy' with NO_TRADE if confidence or expectancy is too low
MIN_SAMPLES = 15
MIN_CONFIDENCE = 50.0
MIN_EXPECTANCY = 500.0

def filter_rule(row):
    # Strategy already marked NO_TRADE
    if "NO_TRADE" in row['Best_Strategy']:
       return row['Best_Strategy']
       
    if row['Trade_Count'] < MIN_SAMPLES:
        return f"NO_TRADE (Low Sample: {row['Trade_Count']})"
        
    if row['Confidence_Score'] < MIN_CONFIDENCE:
        return f"NO_TRADE (Low Confidence Score: {row['Confidence_Score']})"
        
    if row['Expectancy_Rs'] < MIN_EXPECTANCY:
       return f"NO_TRADE (Low Expectancy: Rs {row['Expectancy_Rs']})"
        
    return row['Best_Strategy']

df['Filtered_Strategy'] = df.apply(filter_rule, axis=1)

print("\nSample After applying filters:")
print(df[['Symbol', 'Best_Strategy', 'Filtered_Strategy', 'Trade_Count', 'Confidence_Score', 'Expectancy_Rs']].head(15))

# Save output
df.to_csv("data/Master_Regime_Router_Upgraded.csv", index=False)
print("\nSaved sanitized CSV to data/Master_Regime_Router_Upgraded.csv")
