import os
import glob

files = glob.glob('batch*_silo.py')
for f in files:
    if 'bn' in f: continue
    with open(f, 'r') as fp:
        content = fp.read()
    
    # Replace SYMBOL and LOT_SIZE
    content = content.replace("SYMBOL = 'NIFTY'", "SYMBOL = 'BANKNIFTY'")
    # Some files use 75, just to be sure we find it:
    content = content.replace("LOT_SIZE = 75", "LOT_SIZE = 15")
    
    # Replace strike rounding (batch1, 2, 3, 4)
    content = content.replace("round(spot_at.iloc[0]['close'] / 50) * 50", "round(spot_at.iloc[0]['close'] / 100) * 100")
    
    # Replace Offsets and Wing Widths for BANKNIFTY
    # Nifty uses [50, 100, 150], [50, 100, 200], [-50, 0, 50], [50, 100]
    content = content.replace("[50, 100, 150]", "[100, 200, 300]")
    content = content.replace("[-50, 0, 50]", "[-100, 0, 100]")
    content = content.replace("[50, 100, 200]", "[100, 200, 400]")
    content = content.replace("[50, 100]", "[100, 200]")
    
    out_name = f.replace('_silo.py', '_bn_silo.py')
    with open(out_name, 'w') as fp:
        fp.write(content)

print(f"Created BN versions from {len(files)} files.")
