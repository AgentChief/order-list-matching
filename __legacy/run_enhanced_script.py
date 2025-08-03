import sys
import subprocess

def run_script():
    """Run the reconcile_orders_enhanced.py script"""
    print("Running Enhanced Reconciliation Script...")
    result = subprocess.run(
        ["python", "reconcile_orders_enhanced.py"],
        capture_output=True,
        text=True
    )
    
    print("\nSTDOUT:")
    print(result.stdout)
    
    print("\nSTDERR:")
    print(result.stderr)
    
    print(f"\nExit code: {result.returncode}")

if __name__ == "__main__":
    run_script()
