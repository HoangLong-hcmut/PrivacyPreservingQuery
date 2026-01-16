import pytest
import time
from src.main import execute_secure_query
from src.db_connector import execute_query

# Use a valid ID from the seed (Researcher)
USER_ID = '001075000003'

def test_performance_benchmark(budget_tracker, metrics_recorder):
    """
    Benchmarks the overhead introduced by the Privacy Framework compared to raw SQL execution.
    """
    query = "SELECT COUNT(*) FROM patients WHERE age > 30"
    epsilon = 1.0
    n_iterations = 20
    
    # Ensure sufficient budget
    execute_query("UPDATE staffs SET privacy_budget = 1000.0 WHERE national_id=%s", (USER_ID,))
    
    # 1. Benchmark Raw SQL
    start_time = time.time()
    for _ in range(n_iterations):
        execute_query(query)
    end_time = time.time()
    
    raw_duration = end_time - start_time
    avg_raw = raw_duration / n_iterations
    
    # 2. Benchmark Privacy Framework
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        start_time_fw = time.time()
        for _ in range(n_iterations):
            execute_secure_query(query, USER_ID, epsilon)
        end_time_fw = time.time()
        
    fw_duration = end_time_fw - start_time_fw
    avg_fw = fw_duration / n_iterations
    
    # Calculate Overhead Ratio
    overhead_diff = (avg_fw - avg_raw) * 1000
    overhead_ratio = avg_fw / avg_raw if avg_raw > 0 else 0
    
    # Report
    print(f"\n--- Performance Benchmark (x{n_iterations} runs) ---")
    print(f"Avg Raw Execution:      {avg_raw*1000:.4f} ms")
    print(f"Avg Secure Execution:   {avg_fw*1000:.4f} ms")
    print(f"Slowdown Factor:        {overhead_ratio:.2f}x")
    
    # Record metrics for json output
    metrics_recorder['perf_avg_raw_ms'] = avg_raw * 1000
    metrics_recorder['perf_avg_secure_ms'] = avg_fw * 1000
    metrics_recorder['perf_overhead_factor'] = overhead_ratio

    # Assertion: Overhead should be reasonable
    assert overhead_diff < 50, f"Performance too slow! Latency Overhead: {overhead_diff:.2f} ms."