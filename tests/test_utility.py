import pytest
import numpy as np
from src.main import execute_secure_query
from src.db_connector import execute_query

# IDs from seed
RESEARCHER_ID = '001075000003'

def test_measure_utility_loss(budget_tracker, metrics_recorder):
    user_id = RESEARCHER_ID
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    epsilon = 1.0
    
    # Retrieve the ground truth via raw DB connection
    raw_result = execute_query(query)
    true_value = list(raw_result[0].values())[0]
    
    # Generate noisy samples
    noisy_values = []
    n_iterations = 100
    
    # Increase budget to support multiple iterations
    execute_query("UPDATE staff SET privacy_budget = 1000.0 WHERE national_id = %s", (user_id,))
    
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

    # Record metrics
    metrics_recorder['average_utility_loss_mae'] = float(mae)
    metrics_recorder['average_utility_loss_re'] = float(re)
    metrics_recorder['statistical_bias'] = float(bias)

    print(f"\nTrue Value: {true_value}")
    print(f"MAE: {mae}")
    print(f"RE: {re}")
    print(f"Bias: {bias}")
    
    # Assertions
    # MAE Check
    scale = 1.0 / epsilon
    assert mae < scale * 2.0, f"Absolute Error {mae} is too high compared to theoretical {scale}"

    # Relative Error Check (< 0.1 for this dataset size)
    assert re < 0.1, f"Relative Error {re} is too high (Expected < 0.1)"
    
    # Bias Check (Should be close to 0)
    assert abs(bias) < 0.5 * scale, f"Bias {bias} is significant (Non-zero mean error)"
