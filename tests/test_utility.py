import pytest
import numpy as np
from src.main import execute_secure_query
from src.db_connector import execute_query

def test_measure_utility_loss(budget_tracker, metrics_recorder):
    user_id = "test_user_utility"
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    epsilon = 1.0
    
    # 1. Get True Value
    # We bypass the middleware and use the raw connector (which is mocked by db_session)
    # The mock returns a list of dicts
    raw_result = execute_query(query)
    true_value = list(raw_result[0].values())[0]
    
    # 2. Get Noisy Values
    noisy_values = []
    n_iterations = 100
    
    # We need to reset budget or give enough budget
    budget_tracker.EPSILON_TOTAL = 1000.0 
    
    # Patch the global budget tracker in main to use our fixture
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        for _ in range(n_iterations):
            result = execute_secure_query(query, user_id, epsilon)
            # Result is [{'COUNT(*)': val}]
            val = list(result[0].values())[0]
            noisy_values.append(val)
            
    # 3. Calculations
    noisy_values = np.array(noisy_values)
    mae = np.mean(np.abs(noisy_values - true_value))
    re = np.mean(np.abs(noisy_values - true_value) / (true_value + 1e-9)) # Avoid div by zero
    
    # Record metrics
    metrics_recorder['average_utility_loss_mae'] = float(mae)
    metrics_recorder['average_utility_loss_re_percent'] = float(re * 100)

    print(f"\nTrue Value: {true_value}")
    print(f"Mean Noisy Value: {np.mean(noisy_values)}")
    print(f"MAE: {mae}")
    print(f"RE: {re}")
    
    # 4. Assertions
    # Theoretical MAE for Laplace(1/epsilon) is 1/epsilon.
    # With sensitivity 1, scale = 1/epsilon = 1.
    # MAE of Laplace(b) is b. So expected MAE is 1.0.
    # We allow some margin for randomness (e.g. 3 sigma or just a loose bound).
    # Let's say MAE should be < 2.0 for epsilon=1.0
    
    expected_mae = 1.0 / epsilon
    assert mae < expected_mae * 2.0, f"MAE {mae} is too high compared to theoretical {expected_mae}"
    
    # Bias Check: Mean of noise should be close to 0, so Mean Noisy should be close to True
    bias = np.abs(np.mean(noisy_values) - true_value)
    assert bias < expected_mae * 1.5, f"Bias {bias} is too high"
