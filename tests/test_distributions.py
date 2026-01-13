import pytest
from scipy import stats
from src.main import execute_secure_query
from src.db_connector import execute_query
from src.pipeline.budget import BudgetExhaustedException

def test_validate_laplace_distribution(budget_tracker):
    user_id = "researcher"
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    epsilon = 1.0
    
    # Retrieve true value for baseline
    raw_result = execute_query(query)
    true_value = list(raw_result[0].values())[0]
    
    # Generate samples
    n_samples = 200
    samples = []
    
    # Ensure sufficient budget
    execute_query("UPDATE staff SET privacy_budget = 1000.0 WHERE role = %s", (user_id,))
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        # Mock post-processing to test raw continuous Laplace distribution (disabling rounding)
        # The KS-test is designed for continuous distributions; rounding introduces discretization artifacts.
        with m.context() as m2:
            m2.setattr("src.pipeline.dp_engine.post_process_result", lambda val, type: val)
            
            for _ in range(n_samples):
                result = execute_secure_query(query, user_id, epsilon)
                val = list(result[0].values())[0]
                samples.append(val)

    # Statistical Validation (Kolmogorov-Smirnov Test)
    # Null Hypothesis: The samples follow a Laplace distribution (loc=true_value, scale=1/epsilon)
    scale = 1.0 / epsilon
    ks_statistic, p_value = stats.kstest(samples, 'laplace', args=(true_value, scale))
    
    print(f"\nKS Statistic: {ks_statistic}")
    print(f"P-Value: {p_value}")
    
    # Assertion
    # p-value > 0.05 means we cannot reject the null hypothesis (that it IS Laplace)
    assert p_value > 0.05, f"Samples do not follow Laplace distribution (p={p_value})"

def test_budget_accounting(budget_tracker):
    user_id = "doctor"
    query = "SELECT COUNT(*) FROM patients"
    epsilon_cost = 1.0
    
    # Set known budget
    execute_query("UPDATE staff SET privacy_budget = 5.0 WHERE role = %s", (user_id,))
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        # Run 5 queries
        for i in range(5):
            execute_secure_query(query, user_id, epsilon_cost)
            expected_remaining = 5.0 - (i + 1) * epsilon_cost
            current_remaining = budget_tracker.get_budget(user_id)
            assert abs(current_remaining - expected_remaining) < 1e-9
            
        # Run 6th query - should fail
        with pytest.raises(BudgetExhaustedException):
            execute_secure_query(query, user_id, epsilon_cost)
