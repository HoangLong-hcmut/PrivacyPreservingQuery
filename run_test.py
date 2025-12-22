import subprocess
import sys

def main():
    # Run pytest
    try:
        subprocess.run([sys.executable, "-m", "pytest", "tests"], capture_output=False)
    except Exception as e:
        print(f"Error running tests: {e}")
        
    # Generate Report
    try:
        from src.utils import test_reporter
        test_reporter.generate_report()
    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    main()
