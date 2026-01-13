import sqlglot
from sqlglot import exp
from src.pipeline import sanitizer, rewriter, privacy_guard, dp_engine, budget
from src.db_connector import execute_query
import sys

# Global budget tracker instance
budget_tracker = budget.BudgetAccountant()

class PrivacyMiddleware:
    def __init__(self):
        self.budget_accountant = budget.BudgetAccountant()

    def _detect_query_type(self, parsed_query) -> str:
        """
        Detects if the query is COUNT, SUM, etc.
        """
        expressions = parsed_query.expressions
        if not expressions:
            return "UNKNOWN"
        
        first_expr = expressions[0]
        
        if isinstance(first_expr, exp.Count):
            return "COUNT"
        if isinstance(first_expr, exp.Sum):
            return "SUM"
            
        if isinstance(first_expr, exp.Alias):
            if isinstance(first_expr.this, exp.Count):
                return "COUNT"
            if isinstance(first_expr.this, exp.Sum):
                return "SUM"
                
        return "UNKNOWN"

    def process_query(self, user_query: str, user_id: str, epsilon_cost: float):
        """
        Executes the privacy pipeline: validation -> accounting -> rewriting -> k-anonymity -> differential privacy.
        """
        # 1. Validation: Whitelist checks (schema, attributes, predicates)
        sanitizer.validate_query(user_query)

        # 2. Budget Check: Verify sufficiency before processing
        self.budget_accountant.check(user_id, epsilon_cost)

        # 3. Rewriting: Generalization and Aggregation Enforcement
        generalized_query = rewriter.generalize_filters(user_query)
        target_query = rewriter.enforce_aggregation(generalized_query)
        
        # 4. Cohort Analysis: Check k-Anonymity (k=5)
        if privacy_guard.check_cohort_violation(target_query):
            raise privacy_guard.PrivacyViolationException("Query violates cohort size requirements (k=5).")

        # 5. Differential Privacy Execution
        raw_results = execute_query(target_query)
        
        # Extract scalar value
        true_val = float(list(raw_results[0].values())[0]) if raw_results else 0.0

        # Calculate Sensitivity
        parsed = sqlglot.parse_one(target_query)
        query_type = self._detect_query_type(parsed)
        bounds = (0, 100) if query_type == 'SUM' else None
        sensitivity = dp_engine.calculate_sensitivity(query_type, bounds)

        # Inject Laplace Noise
        final_val = dp_engine.add_noise(true_val, sensitivity, epsilon_cost)
        final_val = dp_engine.post_process_result(final_val, query_type)

        # Commit Budget deduction
        self.budget_accountant.consume_budget(user_id, epsilon_cost)

        return {
            "status": "success",
            "original_query": user_query,
            "executed_query": target_query,
            "result": final_val,
            "epsilon_used": epsilon_cost
        }

# Initialize Middleware with the global tracker for test compatibility
middleware = PrivacyMiddleware()
middleware.budget_accountant = budget_tracker

def execute_secure_query(user_query: str, user_id: str, epsilon_cost: float):
    """
    Executes a secure SQL query via PrivacyMiddleware.
    """
    # Ensure middleware uses the current global budget_tracker (which might be mocked by tests)
    middleware.budget_accountant = budget_tracker

    # Execute through middleware
    result_data = middleware.process_query(user_query, user_id, epsilon_cost)
    
    # Format result to match what tests expect: list of dicts with one key pointing to the value
    # The tests check for ANY key having the value.
    return [{"aggregated_result": result_data["result"]}]


if __name__ == "__main__":
    import argparse
    from src.pipeline.sanitizer import SecurityException
    from src.pipeline.privacy_guard import PrivacyViolationException
    from src.pipeline.budget import BudgetExhaustedException

    parser = argparse.ArgumentParser(description="DataSHIELD Secure Query Interface - Privacy-Preserving SQL Execution Engine")
    parser.add_argument("--role", type=str, default="cli_user", help="User ID / Role (default: cli_user)")
    parser.add_argument("--query", type=str, help="SQL Query to execute (if not provided, enters interactive mode)")
    parser.add_argument("--epsilon", type=float, default=1.0, help="Privacy Loss Budget (epsilon) cost (default: 1.0)")
    
    args = parser.parse_args()

    # If query is provided via command line, run once and exit
    if args.query:
        try:
            print(f"Executing Query as '{args.role}' with epsilon={args.epsilon}...")
            response = middleware.process_query(args.query, args.role, epsilon_cost=args.epsilon)
            
            print("-" * 30)
            print("PASSED")
            print("-" * 30)
            print(f"Result:         {response['result']}")
            print("-" * 30)
        except Exception as e:
            print(f"(!) FAILED: {e}")
            sys.exit(1)
        sys.exit(0)

    # Interactive Mode (Fallback)
    print("="*50)
    print("      DataSHIELD Privacy Middleware CLI      ")
    print("="*50)
    print("Type 'exit' or 'quit' to stop.")
    
    # Default user if not specified
    current_user = args.role
    
    while True:
        try:
            # Check remaining budget directly from DB
            remaining = budget_tracker.get_budget(current_user)
            print(f"\n[User: {current_user}] Budget Remaining: {remaining:.2f}")
            
            query = input("SQL > ").strip()
            
            if query.lower() in ('exit', 'quit'):
                print("Exiting...")
                break
                
            if not query:
                continue
                
            if query.startswith("user:"):
                current_user = query.split(":")[1].strip()
                print(f"Switched to user: {current_user}")
                continue

            # Execute
            try:
                # We use the middleware directly to get detailed response
                response = middleware.process_query(query, current_user, epsilon_cost=args.epsilon)
                
                print("-" * 30)
                print("PASSED")
                print("-" * 30)
                print(f"Original Query: {response['original_query']}")
                print(f"Executed Query: {response['executed_query']}")
                print(f"Result:         {response['result']}")
                print(f"Cost (Epsilon): {response['epsilon_used']}")
                print("-" * 30)
                
            except SecurityException as e:
                print(f"(!) BLOCKED [Security]: {e}")
            except PrivacyViolationException as e:
                print(f"(!) BLOCKED [Privacy]: {e}")
            except BudgetExhaustedException as e:
                print(f"(!) BLOCKED [Budget]: {e}")
            except Exception as e:
                print(f"(!) ERROR: {e}")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
