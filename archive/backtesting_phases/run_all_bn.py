import subprocess
import sys

scripts = ['batch1_bn_silo.py', 'batch2_bn_silo.py', 'batch3_bn_silo.py', 'batch4_bn_silo.py']

for script in scripts:
    print(f"Running {script}...")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error in {script}:\n{result.stderr}")
    else:
        print(f"Successfully finished {script}")
        print(result.stdout[-500:])

print("All BANKNIFTY silos complete!")
