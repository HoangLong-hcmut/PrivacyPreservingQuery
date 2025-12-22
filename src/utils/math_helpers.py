import numpy as np

def clamp_value(value: float, min_val: float = 0.0, max_val: float = None) -> float:
    """
    Clamps a value between a minimum and an optional maximum.
    """
    if value < min_val:
        return min_val
    if max_val is not None and value > max_val:
        return max_val
    return value

def round_to_nearest_int(value: float) -> int:
    """
    Rounds a float to the nearest integer.
    """
    return int(np.round(value))
