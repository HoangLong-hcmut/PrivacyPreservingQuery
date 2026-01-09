import pytest
from src.pipeline.budget import BudgetAccountant, BudgetExhaustedException
from src.db_connector import execute_query

@pytest.fixture
def clean_db_budget():
    # Reset budgets to 10.0 for all test roles
    execute_query("UPDATE staff SET privacy_budget = 10.0 WHERE role IN ('doctor', 'researcher', 'accounting')")
    yield
    # Reset again
    execute_query("UPDATE staff SET privacy_budget = 10.0 WHERE role IN ('doctor', 'researcher', 'accounting')")

def test_persistence_db_update(clean_db_budget):
    acc = BudgetAccountant()
    user_role = "doctor"
    
    # Initial check
    initial = acc.get_budget(user_role)
    assert abs(initial - 10.0) < 0.1
    
    # Consume
    acc.consume_budget(user_role, 2.5)
    
    # Check DB directly
    rows = execute_query("SELECT privacy_budget FROM staff WHERE role = %s", (user_role,))
    new_budget = float(rows[0]['privacy_budget'])
    assert abs(new_budget - 7.5) < 0.1
    
    # Check via accountant
    assert abs(acc.get_budget(user_role) - 7.5) < 0.1

def test_shared_role_budget(clean_db_budget):
    """
    Verifies that budget consumption affects the shared role account correctly.
    Note: Current implementation assumes a shared budget model per role (e.g., all 'doctors' share one budget pool).
    """
    
    acc = BudgetAccountant()
    # Consume for "doctor"
    acc.consume_budget("doctor", 3.0)
    
    # Verify all "doctor" rows have 7.0
    rows = execute_query("SELECT privacy_budget FROM staff WHERE role='doctor'")
    for row in rows:
        assert abs(float(row['privacy_budget']) - 7.0) < 0.1

def test_budget_exhaustion_db(clean_db_budget):
    acc = BudgetAccountant()
    user_role = "accounting"
    
    # 3.0 cost. Budget 10.0 (reset by fixture).
    acc.consume_budget(user_role, 3.0) # Rem: 7.0
    acc.consume_budget(user_role, 3.0) # Rem: 4.0
    acc.consume_budget(user_role, 3.0) # Rem: 1.0
    
    # Next 2.0 should fail
    with pytest.raises(BudgetExhaustedException):
        acc.check(user_role, 2.0)
        
    # State should remain 1.0
    assert abs(acc.get_budget(user_role) - 1.0) < 0.1
