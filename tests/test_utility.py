import pytest
import numpy as np
from src.main import execute_secure_query
from src.db_connector import execute_query

def test_measure_utility_loss(budget_tracker, metrics_recorder):
    user_id = "researcher"
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    epsilon = 1.0
    
    # Retrieve the ground truth via raw DB connection
    raw_result = execute_query(query)
    true_value = list(raw_result[0].values())[0]
    
    # Generate noisy samples
    noisy_values = []
    n_iterations = 100
    
    # Increase budget to support multiple iterations
    execute_query("UPDATE staff SET privacy_budget = 1000.0 WHERE role = %s", (user_id,))
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        for _ in range(n_iterations):
            result = execute_secure_query(query, user_id, epsilon)
            val = list(result[0].values())[0]
            noisy_values.append(val)
            
    # Calculate Metrics
    noisy_values = np.array(noisy_values)
    mae = np.mean(np.abs(noisy_values - true_value))
    re = np.mean(np.abs(noisy_values - true_value) / (true_value + 1e-9))
    
    # Calculate Statistical Properties
    bias = np.mean(noisy_values - true_value)
    
    # Laplace Variance = 2 * (scale)^2
    variance = np.var(noisy_values)
    expected_variance = 2 * (1.0 / epsilon)**2

    # Record metrics
    metrics_recorder['average_utility_loss_mae'] = float(mae)
    metrics_recorder['average_utility_loss_re_percent'] = float(re * 100)
    metrics_recorder['statistical_bias'] = float(bias)
    metrics_recorder['statistical_variance'] = float(variance)

    print(f"\nTrue Value: {true_value}")
    print(f"Mean Noisy Value: {np.mean(noisy_values)}")
    print(f"MAE: {mae}")
    print(f"RE: {re}")
    print(f"Bias: {bias}")
    print(f"Variance: {variance} (Expected: {expected_variance})")
    
    # Assertions
    # MAE Check
    expected_mae = 1.0 / epsilon
    assert mae < expected_mae * 2.0, f"MAE {mae} is too high compared to theoretical {expected_mae}"
    
    # Bias Check (Should be close to 0)
    assert abs(bias) < 0.5 * expected_mae, f"Bias {bias} is significant (Non-zero mean error)"

    # Variance Check (Should equal 2*b^2 with some margin)
    assert 0.5 * expected_variance < variance < 1.5 * expected_variance, \
        f"Variance {variance} deviates significantly from theoretical {expected_variance}"
