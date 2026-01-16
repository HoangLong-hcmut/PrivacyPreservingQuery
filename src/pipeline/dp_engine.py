import numpy as np

def calculate_sensitivity(query_type: str, bounds: tuple = None) -> float:
    """
    Calculates sensitivity based on query type.
    """
    query_type = query_type.upper()
    if query_type == "COUNT":
        return 1.0
    elif query_type in ["SUM", "MIN", "MAX"]:
        if bounds is None:
            raise ValueError(f"Bounds must be provided for {query_type} queries.")
        lower, upper = bounds
        return float(upper - lower)
    else:
        raise ValueError(f"Unsupported query type for sensitivity analysis: {query_type}")

def add_noise(value: float, sensitivity: float, epsilon: float) -> float:
    """
    Adds Laplace noise to the value.
    """
    if epsilon <= 0:
        raise ValueError("Epsilon must be positive.")
    
    scale = sensitivity / epsilon
    noise = np.random.laplace(loc=0.0, scale=scale)
    return value + noise

def post_process_result(noisy_value: float, original_type: str, column_name: str = None) -> float:
    """
    Rounds and clamps the result based on query type and attribute type.
    """
    result = noisy_value
    # Clamping
    if result < 0.0:
        result = 0.0
    
    original_type = original_type.upper()
    
    # 1. COUNT is always integer
    if original_type == "COUNT":
        return int(np.round(result))

    # 2. Heuristic for Integer Attributes (SUM, MIN, MAX)
    is_integer_col = False
    if column_name:
        col = column_name.lower().strip()
        # List of known integer columns from schema
        if col in ["age", "staff_id", "patient_id", "diagnosis_id"]:
            is_integer_col = True
            
    if is_integer_col and original_type in ["SUM", "MIN", "MAX"]:
        return int(np.round(result))
    
    # 3. Default: Float (for AVG, or float columns like privacy_budget)
    return result
