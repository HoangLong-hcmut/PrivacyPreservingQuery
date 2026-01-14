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

def post_process_result(noisy_value: float, original_type: str) -> float:
    """
    Rounds and clamps the result.
    """
    result = noisy_value
    # Clamping
    if result < 0.0:
        result = 0.0
    
    original_type = original_type.upper()
    if original_type == "COUNT":
        # Rounding
        result = int(np.round(result))
    
    return result
