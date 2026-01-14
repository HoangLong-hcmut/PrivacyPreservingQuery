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
        return True

    for row in results:
        count_val = list(row.values())[0]
        if count_val < MIN_COHORT_SIZE:
            return True

    return False
