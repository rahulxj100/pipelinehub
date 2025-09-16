"""
Utility functions for common data processing operations.
"""

from typing import Any, Callable, Dict, List, Union


def filter_numbers(data: List[Union[int, float]], min_val: Union[int, float] = 0) -> List[Union[int, float]]:
    """
    Filter numbers greater than min_val.
    
    Args:
        data: List of numbers
        min_val: Minimum value threshold
        
    Returns:
        Filtered list of numbers
    """
    return [x for x in data if x > min_val]


def square_numbers(data: List[Union[int, float]]) -> List[Union[int, float]]:
    """
    Square all numbers in the data.
    
    Args:
        data: List of numbers
        
    Returns:
        List of squared numbers
    """
    return [x ** 2 for x in data]


def sum_data(data: List[Union[int, float]]) -> Union[int, float]:
    """
    Sum all numbers in the data.
    
    Args:
        data: List of numbers
        
    Returns:
        Sum of all numbers
    """
    return sum(data)


def normalize_data(data: List[Union[int, float]]) -> List[float]:
    """
    Normalize data to 0-1 range.
    
    Args:
        data: List of numbers
        
    Returns:
        Normalized list of numbers
    """
    if not data:
        return data
    min_val, max_val = min(data), max(data)
    if min_val == max_val:
        return [0.0] * len(data)
    return [(x - min_val) / (max_val - min_val) for x in data]


def add_constant(constant: Union[int, float]) -> Callable:
    """
    Create a function that adds a constant to all values.
    
    Args:
        constant: Value to add to each element
        
    Returns:
        Function that adds the constant to data
    """
    def add_const(data: List[Union[int, float]]) -> List[Union[int, float]]:
        return [x + constant for x in data]
    
    add_const.__name__ = f"add_{constant}"
    return add_const


def calculate_stats(data: List[Union[int, float]]) -> Dict[str, Union[int, float]]:
    """
    Calculate basic statistics.
    
    Args:
        data: List of numbers
        
    Returns:
        Dictionary with count, mean, min, max
    """
    if not data:
        return {}
    return {
        'count': len(data),
        'mean': sum(data) / len(data),
        'min': min(data),
        'max': max(data)
    }


def outlier_removal(data: List[Union[int, float]], threshold: float = 2.0) -> List[Union[int, float]]:
    """
    Remove outliers using simple threshold method.
    
    Args:
        data: List of numbers
        threshold: Standard deviation threshold
        
    Returns:
        Data with outliers removed
    """
    if len(data) < 2:
        return data
    
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / len(data)
    std_dev = variance ** 0.5
    
    return [x for x in data if abs(x - mean) <= threshold * std_dev]