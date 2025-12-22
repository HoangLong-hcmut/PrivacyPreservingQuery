import pytest
import numpy as np
from scipy import stats
from src.main import execute_secure_query
from src.db_connector import execute_query
from src.pipeline.budget import BudgetExhaustedException

def test_validate_laplace_distribution(budget_tracker):
    user_id = "test_user_dist"
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    epsilon = 1.0
    
    # Get True Value
    raw_result = execute_query(query)
    true_value = list(raw_result[0].values())[0]
    
    # Generate samples
    n_samples = 200 # Reduced for real DB performance
    samples = []
    
    budget_tracker.EPSILON_TOTAL = float(n_samples * epsilon) + 100.0
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        # We need to disable rounding/clamping to test the raw noise distribution?
        # The prompt says "Post-Process: Round/Clamp".
        # If we round, it becomes discrete Laplace (Geometric).
        # KS-test is for continuous distributions.
        # If the output is rounded, KS-test against continuous Laplace might fail or be inaccurate.
        # However, the prompt asks to "Validate Laplace Distribution" using KS-test.
        # I should probably mock `post_process_result` to return the raw noisy value for this test,
        # OR test against the discrete distribution.
        # Given the prompt "Null Hypothesis: The samples are drawn from a Laplace distribution",
        # I will mock `post_process_result` to return the raw value (skip rounding) for this test.
        
        with m.context() as m2:
            # Mock post_process to return raw value
            # We need to import the module where it is defined or used.
            # It is used in src.main as dp_engine.post_process_result
            # So we mock src.pipeline.dp_engine.post_process_result
            m2.setattr("src.pipeline.dp_engine.post_process_result", lambda val, type: val)
            
            for _ in range(n_samples):
                result = execute_secure_query(query, user_id, epsilon)
                val = list(result[0].values())[0]
                samples.append(val)

    # Statistical Test
    # Laplace(loc=true_value, scale=1/epsilon)
    # Scipy laplace uses loc and scale.
    # Sensitivity is 1. Scale = 1/epsilon.
    scale = 1.0 / epsilon
    
    # KS Test
    # We compare the samples against the CDF of the expected Laplace distribution
    ks_statistic, p_value = stats.kstest(samples, 'laplace', args=(true_value, scale))
    
    print(f"\nKS Statistic: {ks_statistic}")
    print(f"P-Value: {p_value}")
    
    # Assertion
    # p-value > 0.05 means we cannot reject the null hypothesis (that it IS Laplace)
    assert p_value > 0.05, f"Samples do not follow Laplace distribution (p={p_value})"

def test_budget_accounting(budget_tracker):
    user_id = "test_user_budget"
    query = "SELECT COUNT(*) FROM patients"
    epsilon_cost = 1.0
    budget_tracker.EPSILON_TOTAL = 5.0
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        # Run 5 queries
        for i in range(5):
            execute_secure_query(query, user_id, epsilon_cost)
            expected_remaining = 5.0 - (i + 1) * epsilon_cost
            current_usage = budget_tracker.usage.get(user_id, 0.0)
            assert abs((5.0 - current_usage) - expected_remaining) < 1e-9
            
        # Run 6th query - should fail
        with pytest.raises(BudgetExhaustedException):
            execute_secure_query(query, user_id, epsilon_cost)
