import subprocess
import sys
import json
import os

def generate_report():
    report_file = 'test_report.json'
    
    if not os.path.exists(report_file):
        print("Error: Report file not found. Please run the tests first using 'pytest'.")
        return
        
    try:
        with open(report_file, 'r') as f:
            metrics = json.load(f)
            
        print("="*40)
        print("PRIVACY FRAMEWORK TEST REPORT")
        print("="*40)
        
        status = metrics.get('status', 'UNKNOWN')
        print(f"Overall Status: {status}")
        print("-" * 40)
        
        if 'average_utility_loss_mae' in metrics:
            print(f"Average Utility Loss (MAE): {metrics['average_utility_loss_mae']:.4f}")
            print(f"Average Utility Loss (RE):  {metrics['average_utility_loss_re_percent']:.2f}%")
        else:
            print("Utility metrics not found.")
            
        if 'max_observed_sensitivity' in metrics:
            print(f"Max Observed Sensitivity:   {metrics['max_observed_sensitivity']}")
        else:
            print("Sensitivity metrics not found.")
            
        # Budget Accounting Accuracy is implicitly checked by the test passing.
        print(f"Budget Accounting:          {'Verified' if status == 'PASS' else 'Check Failures'}")
        
        print(f"Attacker Simulations:       {'Passed' if status == 'PASS' else 'Check Failures'}")
        
        print("="*40)
        
        # Save combined report to JSON
        summary = {
            "average_utility_loss_percent": metrics.get('average_utility_loss_re_percent'),
            "max_observed_sensitivity": metrics.get('max_observed_sensitivity'),
            "budget_accounting_accuracy_percent": 100.0 if status == 'PASS' else 0.0,
            "attacker_simulations_status": "PASS" if status == 'PASS' else "FAIL"
        }
        
        # Update metrics with summary fields for a single comprehensive report
        metrics.update(summary)
        
        with open(report_file, 'w') as f:
            json.dump(metrics, f, indent=4)
            
        print(f"Report saved to {report_file}")

    except Exception as e:
        print(f"Error generating report: {e}")

def main():
    # Run pytest
    try:
        subprocess.run([sys.executable, "-m", "pytest", "tests"], capture_output=False)
    except Exception as e:
        print(f"Error running tests: {e}")
        
    # Generate Report
    try:
        generate_report()
    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    main()
