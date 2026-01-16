import pytest
from scipy import stats
from src.main import execute_secure_query
from src.db_connector import execute_query

# Valid IDs from seed
RESEARCHER_ID = '001075000003'

def test_validate_laplace_distribution(budget_tracker, metrics_recorder):
    user_id = RESEARCHER_ID
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    epsilon = 1.0
    
    # Retrieve true value for baseline
    raw_result = execute_query(query)
    true_value = list(raw_result[0].values())[0]
    
    # Generate samples
    n_samples = 200
    samples = []
    
    # Ensure sufficient budget
    execute_query("UPDATE staffs SET privacy_budget = 1000.0 WHERE national_id = %s", (user_id,))
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        # Mock post-processing to test raw continuous Laplace distribution (disabling rounding)
        with m.context() as m2:
            m2.setattr("src.pipeline.dp_engine.post_process_result", lambda val, type, col=None: val)
            
            for _ in range(n_samples):
                result = execute_secure_query(query, user_id, epsilon)
                val = list(result[0].values())[0]
                samples.append(val)

    # Statistical Validation (Kolmogorov-Smirnov Test)
    scale = 1.0 / epsilon
    ks_statistic, p_value = stats.kstest(samples, 'laplace', args=(true_value, scale))
    
    print(f"\nKS Statistic: {ks_statistic}")
    print(f"P-Value: {p_value}")

    metrics_recorder['dp_ks_statistic'] = float(ks_statistic)
    metrics_recorder['dp_ks_p_value'] = float(p_value)
    
    # p-value > 0.05 means we cannot reject the null hypothesis (that it IS Laplace)
    assert p_value > 0.05, f"Samples do not follow Laplace distribution (p={p_value})"

