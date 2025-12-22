import numpy as np
from src.utils.math_helpers import round_to_nearest_int, clamp_value

def calculate_sensitivity(query_type: str, bounds: tuple = None) -> float:
    """
    Calculates sensitivity based on query type.
    """
    query_type = query_type.upper()
    if query_type == "COUNT":
        return 1.0
    elif query_type == "SUM":
        if bounds is None:
            raise ValueError("Bounds must be provided for SUM queries.")
        lower, upper = bounds
        return float(upper - lower)
    else:
        # Default or unknown, maybe raise error or assume high sensitivity?
        # For this spec, let's raise.
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
    original_type = original_type.upper()
    
    # Clamping: Ensure counts are not negative
    # The prompt says "Clamping: Ensure counts are not negative (replace < 0 with 0)"
    # It implies this is mostly for counts.
    
    result = noisy_value
    
    if original_type == "COUNT":
        result = clamp_value(result, min_val=0.0)
        result = round_to_nearest_int(result)
    
    # For SUM, we might not want to clamp to 0 if negative sums are possible, 
    # but if it's something like "Sum of ages", it should be positive.
    # The prompt specifically mentions "If the query was a count, round to the nearest integer."
    # and "Clamping: Ensure counts are not negative".
    
    return result
