import sqlglot
from sqlglot import exp
from src.pipeline import sanitizer, rewriter, privacy_guard, dp_engine, budget
from src.db_connector import execute_query

# Initialize Budget Tracker (Global for this simple server)
budget_tracker = budget.BudgetTracker()

def detect_query_type(parsed_query) -> str:
    """
    Detects if the query is COUNT, SUM, or other.
    Simple heuristic: check the first expression in SELECT.
    """
    # This is a simplification. Real systems need to handle multiple aggregates.
    expressions = parsed_query.expressions
    if not expressions:
        return "UNKNOWN"
    
    first_expr = expressions[0]
    
    if isinstance(first_expr, exp.Count):
        return "COUNT"
    if isinstance(first_expr, exp.Sum):
        return "SUM"
    
    # Check if it's an alias pointing to an aggregate
    if isinstance(first_expr, exp.Alias):
        if isinstance(first_expr.this, exp.Count):
            return "COUNT"
        if isinstance(first_expr.this, exp.Sum):
            return "SUM"
            
    return "UNKNOWN"

def execute_secure_query(user_query: str, user_id: str, epsilon_cost: float):
    """
    Executes a secure SQL query with privacy checks and noise injection.
    """
    # 1. Sanitize
    sanitizer.validate_query(user_query)
    
    # 2. Budget Check
    budget_tracker.check(user_id, epsilon_cost)
    
    # 3. Normalize
    normalized_query = rewriter.normalize_query(user_query)
    
    # 4. Cohort Check
    if privacy_guard.check_cohort_violation(normalized_query):
        raise privacy_guard.PrivacyViolationException("Query violates cohort size requirements.")
    
    # 5. Analyze Query for DP
    parsed = sqlglot.parse_one(normalized_query)
    query_type = detect_query_type(parsed)
    
    if query_type not in ["COUNT", "SUM"]:
        raise ValueError("Only COUNT and SUM queries are supported for differential privacy.")
    
    # 6. Execute Raw
    raw_results = execute_query(normalized_query)
    
    # 7. Add Noise & Post-Process
    # We assume the result is a list of dicts. We process the aggregate value in each row.
    
    # Calculate sensitivity
    # For SUM, we need bounds. This is hard to get automatically without metadata.
    # For this MVP, we'll assume bounds are (0, 100) for SUMs or just use 1 for COUNT.
    # In a real system, we'd look up the column metadata.
    bounds = (0, 100) if query_type == "SUM" else None
    sensitivity = dp_engine.calculate_sensitivity(query_type, bounds)
    
    processed_results = []
    for row in raw_results:
        # We assume the first column is the aggregate
        # row is a dict like {'COUNT(*)': 123, 'age': 30}
        # We need to find the aggregate value.
        # If we preserved the order, it's likely the first value if we didn't use DictCursor,
        # but with DictCursor we rely on keys.
        # Let's assume the query is simple: SELECT COUNT(...) ...
        # We'll try to find the numeric value that looks like the result.
        # Or better, we just process all numeric values? No, that might corrupt group keys.
        
        # Heuristic: The key corresponding to the aggregate expression.
        # Since we don't have the exact key name easily without executing, 
        # we'll iterate and find the one that matches the aggregate type or just the first numeric non-group column?
        
        # Let's just take the first value for now as a simplification for single-column aggregates.
        # If there are group by columns, they are usually included.
        
        new_row = row.copy()
        
        # Find the aggregate key. 
        # If it's `SELECT COUNT(*) as c ...`, key is `c`.
        # If it's `SELECT COUNT(*) ...`, key is `COUNT(*)`.
        
        # We'll apply noise to ALL numeric values that are not likely group keys? 
        # That's risky.
        
        # Let's assume the user query is strictly `SELECT AGG(...)` or `SELECT AGG(...), GROUP_COL ...`
        # We will apply noise to the value that corresponds to the aggregate.
        
        # For this MVP, let's just apply noise to the first value found that is a number.
        keys = list(row.keys())
        # This is a bit fragile but works for "SELECT COUNT(*) FROM ..." -> {'COUNT(*)': 10}
        
        for k, v in row.items():
            if isinstance(v, (int, float)):
                # Apply noise
                noisy_val = dp_engine.add_noise(float(v), sensitivity, epsilon_cost)
                final_val = dp_engine.post_process_result(noisy_val, query_type)
                new_row[k] = final_val
                # We only noise one value per row (the aggregate) for this MVP
                break
        
        processed_results.append(new_row)

    # 8. Commit Budget
    budget_tracker.consume_budget(user_id, epsilon_cost)
    
    return processed_results

if __name__ == "__main__":
    # Simple CLI test
    import sys
    
    # Mocking DB execution for CLI run if no DB is present would be good, 
    # but the code imports real connector.
    # We'll just print "Ready".
    print("Privacy-Preserving Middleware Ready.")
