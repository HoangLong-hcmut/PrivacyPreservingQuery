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
            
        status = metrics.get('status', 'UNKNOWN')
        
        detailed_report = {
            "overall_status": status,
            "correctness_tests": {
                "absolute_error": metrics.get('average_utility_loss_mae'),
                "relative_error": metrics.get('average_utility_loss_re'),
                "statistical_bias": metrics.get('statistical_bias'),
                "empirical_sensitivity": metrics.get('max_observed_sensitivity')
            },
            "privacy_guarantees_validation": {
                "dp_statistical_test": {
                    "ks_statistic": metrics.get('dp_ks_statistic'),
                    "p_value": metrics.get('dp_ks_p_value'),
                    "result": "PASS" if metrics.get('dp_ks_p_value', 0) > 0.05 else "FAIL"
                },
                "budget_accounting_accuracy": metrics.get('budget_accounting_status', 'UNKNOWN')
            },
            "attacker_simulations": {
                "exfiltration_averaging_defense": metrics.get('averaging_defense_status', 'UNKNOWN'),
                "singling_out_prevention": metrics.get('singling_out_status', 'UNKNOWN')
            },
            "system_performance": {
                "avg_raw_execution_ms": metrics.get('perf_avg_raw_ms'),
                "avg_secure_execution_ms": metrics.get('perf_avg_secure_ms'),
                "overhead_latency_factor": metrics.get('perf_overhead_factor')
            }
        }

        with open(report_file, 'w') as f:
            json.dump(detailed_report, f, indent=4)


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
