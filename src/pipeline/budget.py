from src.db_connector import execute_query

class BudgetExhaustedException(Exception):
    pass

class BudgetAccountant:
    def __init__(self):
        pass

    def get_budget(self, user_role: str) -> float:
        """
        Retrieves the remaining budget from the staff table.
        """
        try:
            rows = execute_query("SELECT privacy_budget FROM staff WHERE role = %s", (user_role,))
            if rows:
                return float(rows[0]['privacy_budget'])
            else:
                print(f"Warning: Role '{user_role}' not found in staff table.")
                return 0.0
        except Exception as e:
            print(f"Warning: DB Error fetching budget for {user_role}: {e}")
            return 0.0

    def check(self, user_id: str, cost: float) -> bool:
        """
        Raises BudgetExhaustedException if the remaining budget is insufficient.
        """
        remaining = self.get_budget(user_id)
        
        if remaining < cost:
            raise BudgetExhaustedException(
                f"Budget exhausted. Requested: {cost:.2f}, Remaining: {remaining:.2f}"
            )
        return True

    def consume_budget(self, user_id: str, cost: float):
        """
        Deducts the specified cost from the user's budget in the database.
        """
        self.check(user_id, cost)
        
        try:
            execute_query("""
                UPDATE staff 
                SET privacy_budget = privacy_budget - %s 
                WHERE role = %s
            """, (cost, user_id))
            
        except Exception as e:
            print(f"Error updating budget for {user_id}: {e}")

# Export alias
BudgetTracker = BudgetAccountant
