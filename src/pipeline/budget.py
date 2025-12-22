class BudgetExhaustedException(Exception):
    pass

class BudgetTracker:
    def __init__(self):
        self.usage = {}
        self.EPSILON_TOTAL = 10.0

    def check(self, user_id: str, cost: float) -> bool:
        """
        Checks if the user has enough budget for the query.
        Raises BudgetExhaustedException if not.
        """
        current_usage = self.usage.get(user_id, 0.0)
        if current_usage + cost > self.EPSILON_TOTAL:
            raise BudgetExhaustedException(
                f"Budget exhausted. Requested: {cost}, Available: {self.EPSILON_TOTAL - current_usage}"
            )
        return True

    def consume_budget(self, user_id: str, cost: float):
        """
        Deducts the cost from the user's budget.
        """
        # Check again to be safe, though check() should be called before.
        self.check(user_id, cost)
        self.usage[user_id] = self.usage.get(user_id, 0.0) + cost
