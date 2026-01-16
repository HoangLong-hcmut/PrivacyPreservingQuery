import pytest
import time
from src.pipeline.budget import BudgetAccountant, BudgetExhaustedException
from src.db_connector import execute_query
from src.main import execute_secure_query

# IDs from seed_db.py
DOCTOR_ID = '001080000001'
RESEARCHER_ID = '001075000003'
ACCOUNTING_ID = '001086000009'
ANOTHER_DOCTOR_ID = '001085000005'

@pytest.fixture
def clean_db_budget():
    # Reset budgets to 10.0 for all test users
    all_test_ids = [DOCTOR_ID, RESEARCHER_ID, ACCOUNTING_ID, ANOTHER_DOCTOR_ID]
    placeholders = ', '.join(['%s'] * len(all_test_ids))
    execute_query(f"UPDATE staffs SET privacy_budget = 10.0 WHERE national_id IN ({placeholders})", tuple(all_test_ids))


def test_persistence_db_update(clean_db_budget):
    acc = BudgetAccountant()
    user_id = DOCTOR_ID
    
    # Initial check
    initial = acc.get_budget(user_id)
    assert abs(initial - 10.0) < 0.1
    
    # Consume
    acc.consume_budget(user_id, 2.5)
    
    # Check DB directly
    rows = execute_query("SELECT privacy_budget FROM staffs WHERE national_id = %s", (user_id,))
    new_budget = float(rows[0]['privacy_budget'])
    assert abs(new_budget - 7.5) < 0.1
    
    # Check via accountant
    assert abs(acc.get_budget(user_id) - 7.5) < 0.1

def test_individual_user_budget_isolation(clean_db_budget):
    """
    Verifies that consuming budget for a specific National ID only affects that user,
    NOT others with the same role.
    """
    acc = BudgetAccountant()
    # Both start at 10.0
    assert abs(acc.get_budget(DOCTOR_ID) - 10.0) < 0.1
    assert abs(acc.get_budget(ANOTHER_DOCTOR_ID) - 10.0) < 0.1

    # Consume 4.0 from Target
    acc.consume_budget(DOCTOR_ID, 4.0)

    # Verify Target is now 6.0
    assert abs(acc.get_budget(DOCTOR_ID) - 6.0) < 0.1

    # Verify Other Doctor is STILL 10.0 (NOT deducted)
    assert abs(acc.get_budget(ANOTHER_DOCTOR_ID) - 10.0) < 0.1

def test_budget_exhaustion_db(clean_db_budget):
    acc = BudgetAccountant()
    user_id = ACCOUNTING_ID
    
    # 3.0 cost. Budget 10.0 (reset by fixture).
    acc.consume_budget(user_id, 3.0) # Rem: 7.0
    acc.consume_budget(user_id, 3.0) # Rem: 4.0
    acc.consume_budget(user_id, 3.0) # Rem: 1.0
    
    # Next 2.0 should fail
    with pytest.raises(BudgetExhaustedException):
        acc.check(user_id, 2.0)
        
    # State should remain 1.0
    assert abs(acc.get_budget(user_id) - 1.0) < 0.1

def test_budget_accounting(budget_tracker):
    user_id = DOCTOR_ID
    query = "SELECT COUNT(*) FROM patients"
    epsilon_cost = 1.0
    
    # Set known budget
    execute_query("UPDATE staffs SET privacy_budget = 5.0 WHERE national_id = %s", (user_id,))
    
    with pytest.MonkeyPatch.context() as m:
        m.setattr("src.main.budget_tracker", budget_tracker)
        
        # Run 5 queries
        for i in range(5):
            execute_secure_query(query, user_id, epsilon_cost)
            time.sleep(0.1) # Wait for async budget update
            expected_remaining = 5.0 - (i + 1) * epsilon_cost
            current_remaining = budget_tracker.get_budget(user_id)
            assert abs(current_remaining - expected_remaining) < 1e-9
            
        # Run 6th query - should fail
        with pytest.raises(BudgetExhaustedException):
            execute_secure_query(query, user_id, epsilon_cost)

