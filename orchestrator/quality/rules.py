"""
Individual data quality rules.
Each rule takes a Polars DataFrame and returns (passed: bool, message: str).
"""

from typing import List, Tuple

import polars as pl


def check_no_nulls(df: pl.DataFrame, columns: List[str]) -> Tuple[bool, str]:
    """
    Check that specified columns have no null values.
    
    Returns:
        (passed, failure_message)
    """
    failures = []
    for col in columns:
        null_count = df.select(pl.col(col).null_count()).item()
        if null_count > 0:
            failures.append(f"Column '{col}' has {null_count} nulls")
    
    if failures:
        return False, "; ".join(failures)
    return True, "OK"


def check_positive_values(df: pl.DataFrame, column: str = "price") -> Tuple[bool, str]:
    """
    Check that price values are positive.
    
    Returns:
        (passed, failure_message)
    """
    invalid = df.filter(pl.col(column) <= 0)
    if invalid.height > 0:
        return False, f"Column '{column}' has {invalid.height} non-positive values"
    return True, "OK"


def check_valid_timestamps(df: pl.DataFrame, column: str = "timestamp_ms") -> Tuple[bool, str]:
    """
    Check that timestamps are valid positive integers.
    
    Returns:
        (passed, failure_message)
    """
    invalid = df.filter(pl.col(column) <= 0)
    if invalid.height > 0:
        return False, f"Column '{column}' has {invalid.height} invalid timestamps (≤ 0)"
    
    # Check reasonable range (year 2020+)
    min_valid_ts = 1577836800000  # 2020-01-01 in ms
    too_old = df.filter(pl.col(column) < min_valid_ts)
    if too_old.height > 0:
        return False, f"Column '{column}' has {too_old.height} timestamps before 2020"
    
    return True, "OK"


def check_no_empty(df: pl.DataFrame) -> Tuple[bool, str]:
    """
    Check that DataFrame is not empty.
    
    Returns:
        (passed, failure_message)
    """
    if df.is_empty():
        return False, "DataFrame is empty"
    return True, "OK"