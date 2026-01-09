import pytest
import numpy as np
from src.pipeline.budget import BudgetTracker

@pytest.fixture
def dp_engine_fixed_seed():
    """
    Ensure deterministic results by fixing the random seed.
    """
    np.random.seed(42)
    yield
    np.random.seed(None)

@pytest.fixture
def budget_tracker():
    """
    Provide a fresh, isolated budget tracker instance.
    """
    tracker = BudgetTracker()
    return tracker

# Metrics collection
TEST_METRICS = {}

@pytest.fixture
def metrics_recorder():
    return TEST_METRICS

def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finishes.
    """
    import json
    TEST_METRICS['status'] = 'PASS' if exitstatus == 0 else 'FAIL'
    
    with open('test_report.json', 'w') as f:
        json.dump(TEST_METRICS, f, indent=4)
