from src.pipeline.rewriter import rewrite_for_count
from src.db_connector import execute_query

MIN_COHORT_SIZE = 10

class PrivacyViolationException(Exception):
    pass

def check_cohort_violation(sql: str) -> bool:
    """
    Checks if the query violates the minimum cohort size requirement.
    Returns True if blocked (violation), False otherwise.
    """
    # 1. Rewrite query to count cohort size
    count_sql = rewrite_for_count(sql)
    
    # 2. Execute against DB
    # We use the existing db_connector.execute_query which handles connection
    results = execute_query(count_sql)
    
    # 3. Check results
    # results is a list of dicts, e.g., [{'COUNT(DISTINCT patient_id)': 15}, ...]
    # We need to check if ANY of the counts is < MIN_COHORT_SIZE
    
    if not results:
        # Empty result means 0 count, which is < 10.
        # But if the query returns no rows, does it violate privacy?
        # Usually empty set is safe, but it reveals that no one matches.
        # If we are strict, 0 < 10, so it's a violation if we consider "singling out" as "narrowing down too much".
        # However, standard k-anonymity usually applies to non-empty sets.
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
