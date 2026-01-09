from src.pipeline.rewriter import rewrite_for_count
from src.db_connector import execute_query

MIN_COHORT_SIZE = 5

class PrivacyViolationException(Exception):
    pass

def check_cohort_violation(sql: str) -> bool:
    """
    Checks if the query result size is below the minimum required threshold.
    Returns True if violation detected.
    """
    # Rewrite to get size count
    count_sql = rewrite_for_count(sql)
    
    # Execute check
    results = execute_query(count_sql)
    
    if not results:
        # Empty results imply a group size of 0, which violates k-anonymity if k > 0

        # But for differential privacy, we usually add noise to the count.
        # This "Cohort Bounding" is a separate check.
        # If I ask "SELECT * FROM patients WHERE id = 123", count is 1. Violation.
        # If I ask "SELECT * FROM patients WHERE age = 1000", count is 0.
        # If I return empty set, I reveal no one has age 1000.
        # Let's assume 0 is safe or handle it as < MIN.
        # The prompt says "If result < MIN_COHORT_SIZE, return True". 0 is < 10.
        # So I will return True.
        return True

    for row in results:
        # The key might vary depending on the driver, but usually it's the expression string.
        # Let's just take the first value of the dict.
        count_val = list(row.values())[0]
        if count_val < MIN_COHORT_SIZE:
            return True

    return False
