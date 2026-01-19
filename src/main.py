import sqlglot
from sqlglot import exp
from src.pipeline import sanitizer, rewriter, privacy_guard, dp_engine, budget
from src.db_connector import execute_query, UsePersistentConnection
import sys
import threading

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
        if isinstance(first_expr, exp.Min):
            return "MIN"
        if isinstance(first_expr, exp.Max):
            return "MAX"
        if isinstance(first_expr, exp.Avg):
            return "AVG"
            
        if isinstance(first_expr, exp.Alias):
            if isinstance(first_expr.this, exp.Count):
                return "COUNT"
            if isinstance(first_expr.this, exp.Sum):
                return "SUM"
            if isinstance(first_expr.this, exp.Min):
                return "MIN"
            if isinstance(first_expr.this, exp.Max):
                return "MAX"
            if isinstance(first_expr.this, exp.Avg):
                return "AVG"
                
        return "UNKNOWN"

    def _get_target_column(self, parsed_query) -> str:
        """
        Extracts the name of the column being aggregated.
        """
        expressions = parsed_query.expressions
        if not expressions:
            return None
        
        first_expr = expressions[0]
        
        # Unwrap Alias
        if isinstance(first_expr, exp.Alias):
            first_expr = first_expr.this
            
        # Check aggregates
        if isinstance(first_expr, (exp.Count, exp.Sum, exp.Min, exp.Max, exp.Avg)):
            col_node = first_expr.find(exp.Column)
            if col_node:
                return col_node.name
        
        return None

    def _handle_avg_query(self, target_query: str, user_id: str, epsilon_cost: float):
        """
        Handles AVG queries by splitting them into SUM and COUNT.
        """
        # Parse logic to split AVG(col) into SUM(col), COUNT(col)
        parsed = sqlglot.parse_one(target_query)
        avg_expr = parsed.expressions[0]
        
        # Unwrap Alias if present
        if isinstance(avg_expr, exp.Alias):
            avg_expr = avg_expr.this
            
        target_col = avg_expr.this
        
        # Construct dual query
        sum_expr = exp.Sum(this=target_col)
        count_expr = exp.Count(this=target_col)
        parsed.set("expressions", [sum_expr, count_expr])
        dual_query = parsed.sql(dialect="mysql")
        
        # Execute
        raw_results = execute_query(dual_query)
        
        if not raw_results:
            return 0.0
             
        # Extract values
        row = list(raw_results[0].values())
        true_sum = float(row[0]) if row[0] is not None else 0.0
        true_count = float(row[1]) if row[1] is not None else 0.0
        
        # Budget Splitting (50% for Sum, 50% for Count)
        epsilon_half = epsilon_cost / 2.0
        
        # Add Noise to SUM
        bounds = (0, 100)
        sum_sensitivity = dp_engine.calculate_sensitivity("SUM", bounds)
        noisy_sum = dp_engine.add_noise(true_sum, sum_sensitivity, epsilon_half)
        
        # Add Noise to COUNT
        count_sensitivity = 1.0
        noisy_count = dp_engine.add_noise(true_count, count_sensitivity, epsilon_half)
        
        # Post-Process Count
        if noisy_count < 1.0:
            final_avg = 0.0
        else:
            final_avg = noisy_sum / noisy_count
        
        # Commit Budget deduction
        self.budget_accountant.consume_budget(user_id, epsilon_cost)

        return {
            "status": "success",
            "original_query": target_query, # Technically original was user_query
            "executed_query": dual_query,
            "result": final_avg,
            "epsilon_used": epsilon_cost,
            "query_type": "AVG"
        }

    def _get_role(self, user_id: str) -> str:
        try:
            res = execute_query("SELECT role, specialization FROM staffs WHERE national_id=%s", (user_id,))
            if not res:
                return "default"
            
            row = res[0]
            role = row['role'].lower()
            spec = row['specialization'].lower()

            # Check for specific job titles first
            if "cashier" in spec:
                return "cashier"
            if "accountant" in spec:
                return "accountant"
            if "receptionist" in spec:
                return "accountant"
            if "secretary" in spec:
                return "default"
            
            # Fallback to the main role
            return role
        except:
            return "default"

    def process_query(self, user_query: str, user_id: str, epsilon_cost: float):
        """
        Executes the privacy pipeline: validation -> accounting -> rewriting -> k-anonymity -> differential privacy.
        """
        with UsePersistentConnection():
            # 0. Get Role
            user_role = self._get_role(user_id)

            # 1. Validation: Whitelist checks (schema, attributes, predicates)
            sanitizer.validate_query(user_query, user_role)

            # 2. Budget Check: Verify sufficiency before processing
            self.budget_accountant.check(user_id, epsilon_cost)

            # 3. Rewriting: Generalization and Aggregation Enforcement
            generalized_query = rewriter.generalize_filters(user_query)
            target_query = rewriter.enforce_aggregation(generalized_query)
            
            # 4. Cohort Analysis: Check k-Anonymity (k=5)
            if privacy_guard.check_cohort_violation(target_query):
                raise privacy_guard.PrivacyViolationException("Query violates cohort size requirements (k=5).")

            # Check for AVG special handling
            parsed_target = sqlglot.parse_one(target_query)
            query_type = self._detect_query_type(parsed_target)
            target_col = self._get_target_column(parsed_target)

            # Handle AVG
            if query_type == "AVG":
                result = self._handle_avg_query(target_query, user_id, epsilon_cost)    
                result["original_query"] = user_query
                return result

            # 5. Differential Privacy Execution
            raw_results = execute_query(target_query)
            
            # Extract scalar value
            true_val = float(list(raw_results[0].values())[0]) if raw_results else 0.0

            # Calculate Sensitivity
            bounds = (0, 100) if query_type in ['SUM', 'MIN', 'MAX'] else None
            sensitivity = dp_engine.calculate_sensitivity(query_type, bounds)

            # Inject Laplace Noise
            final_val = dp_engine.add_noise(true_val, sensitivity, epsilon_cost)
            final_val = dp_engine.post_process_result(final_val, query_type, target_col)

            # 6. Budget Deduction: Commit asynchronously
            def _async_commit():
                try:
                    execute_query(
                        "UPDATE staffs SET privacy_budget = privacy_budget - %s WHERE national_id = %s", 
                        (epsilon_cost, user_id), 
                        force_new=True
                    )
                except Exception as e:
                    print(f"Background Budget Update Failed: {e}")

            threading.Thread(target=_async_commit, daemon=False).start()

            return {
                "status": "success",
                "original_query": user_query,
                "executed_query": target_query,
                "result": final_val,
                "epsilon_used": epsilon_cost,
                "query_type": query_type
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
    return [{"aggregated_result": result_data["result"]}]


if __name__ == "__main__":
    import argparse
    from src.pipeline.sanitizer import SecurityException
    from src.pipeline.privacy_guard import PrivacyViolationException
    from src.pipeline.budget import BudgetExhaustedException

    parser = argparse.ArgumentParser(description="Privacy Preserving SQL Execution Engine")
    parser.add_argument("--user_id", type=str, default="cli_user", help="User (National) ID (default: cli_user)")
    parser.add_argument("--query", type=str, help="SQL Query to execute (if not provided, enters interactive mode)")
    parser.add_argument("--epsilon", type=float, default=1.0, help="Privacy Loss Budget (epsilon) cost (default: 1.0)")
    
    args = parser.parse_args()

    # If query is provided via command line, run once and exit
    if args.query:
        try:
            print(f"Executing Query as '{args.user_id}' with epsilon={args.epsilon}...")
            response = middleware.process_query(args.query, args.user_id, epsilon_cost=args.epsilon)
            
            print("-" * 30)
            print(f"Executed Query: {response['executed_query']}")
            print(f"Privacy Budget Cost (Epsilon): {response['epsilon_used']}")
            
            # Table Formatting
            res_val = str(response['result'])
            col_name = f"DP_{response.get('query_type', 'RESULT')}"
            width = max(len(col_name), len(res_val)) + 4
            
            print(f"+{'-'*width}+")
            print(f"| {col_name:<{width-2}} |")
            print(f"+{'-'*width}+")
            print(f"| {res_val:<{width-2}} |")
            print(f"+{'-'*width}+")
            print("-" * 30)
        except Exception as e:
            print(f"(!) FAILED: {e}")
            sys.exit(1)
        sys.exit(0)

    # Interactive Mode (Fallback)
    print("="*50)
    print("      Privacy Preserving Query Interface CLI      ")
    print("="*50)
    print("Type 'exit' or 'quit' to stop.")
    
    # Default user if not specified
    current_user = args.user_id
    
    while True:
        try:
            # Check remaining budget directly from DB
            remaining = budget_tracker.get_budget(current_user)
            print(f"\n[User: {current_user}] | Budget Remaining: {remaining:.2f} | Privacy Budget Cost per Query: {args.epsilon}")
            
            query = input("SQL > ").strip()
            
            if query.lower() in ('exit', 'quit'):
                print("Exiting...")
                break
                
            if not query:
                continue

            # Support epsilon change in interactive mode
            current_epsilon = args.epsilon
            if query.startswith("epsilon:"):
                try:
                    val = float(query.split(":")[1].strip())
                    if val > 0:
                        args.epsilon = val
                        print(f"Privacy Budget Cost (Epsilon) updated to: {args.epsilon}")
                    else:
                        print("(!) Epsilon must be positive.")
                except ValueError:
                    print("(!) Invalid format. Example 'epsilon: 1.0'")
                continue

            # Execute
            try:
                # We use the middleware directly to get detailed response
                response = middleware.process_query(query, current_user, epsilon_cost=current_epsilon)
                
                print("-" * 30)
                print(f"Executed Query: {response['executed_query']}")
                print(f"Privacy Budget Cost (Epsilon): {response['epsilon_used']}")
                
                # Table Formatting
                res_val = str(response['result'])
                col_name = f"DP_{response.get('query_type', 'RESULT')}"
                width = max(len(col_name), len(res_val)) + 4

                print(f"+{'-'*width}+")
                print(f"| {col_name:<{width-2}} |")
                print(f"+{'-'*width}+")
                print(f"| {res_val:<{width-2}} |")
                print(f"+{'-'*width}+")
                print("-" * 30)
                
            except SecurityException as e:
                print(f"(!) BLOCKED: {e}")
            except PrivacyViolationException as e:
                print(f"(!) BLOCKED: {e}")
            except BudgetExhaustedException as e:
                print(f"(!) BLOCKED: {e}")
            except Exception as e:
                print(f"(!) ERROR: {e}")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
