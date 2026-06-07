"""
Data Quality Runner.
Executes DQ rules against Polars DataFrames and raises DataQualityError on failure.
"""

import logging
from typing import Callable, List, Tuple

import polars as pl

from core.exceptions import DataQualityError

logger = logging.getLogger(__name__)

# Type alias for a DQ rule function
DQRule = Callable[[pl.DataFrame], Tuple[bool, str]]


class DataQualityRunner:
    """
    Executes a set of data quality rules against a DataFrame.
    
    Usage:
        runner = DataQualityRunner()
        runner.add_rule("no_nulls", lambda df: check_no_nulls(df, ["price"]))
        runner.run(df, stage="bronze")
    """
    
    def __init__(self):
        self.rules: List[Tuple[str, DQRule]] = []
    
    def add_rule(self, name: str, rule: DQRule) -> None:
        """Register a DQ rule."""
        self.rules.append((name, rule))
    
    def run(self, df: pl.DataFrame, stage: str = "") -> None:
        """
        Execute all registered rules.
        
        Raises:
            DataQualityError: If any rule fails.
        
        Args:
            df: DataFrame to validate
            stage: Pipeline stage name for error messages
        
        Returns:
            None (raises on failure)
        """
        failures = {}
        
        for rule_name, rule_fn in self.rules:
            try:
                passed, message = rule_fn(df)
                if not passed:
                    failures[rule_name] = message
                    logger.error("[%s] DQ FAILED: %s — %s", stage, rule_name, message)
                else:
                    logger.debug("[%s] DQ PASSED: %s", stage, rule_name)
            except Exception as exc:
                failures[rule_name] = f"Error executing rule: {exc}"
                logger.error("[%s] DQ ERROR: %s — %s", stage, rule_name, exc)
        
        if failures:
            raise DataQualityError(
                f"[{stage}] {len(failures)} DQ check(s) failed",
                failures=failures,
            )
        
        logger.info("✅ [%s] All DQ checks passed (%d rules)", stage, len(self.rules))


def create_bronze_dq_runner() -> DataQualityRunner:
    """Factory: Create a pre-configured DQ runner for Bronze layer."""
    from quality.rules import (
        check_no_nulls,
        check_positive_values,
        check_valid_timestamps,
        check_no_empty,
    )
    
    runner = DataQualityRunner()
    runner.add_rule("no_empty", check_no_empty)
    runner.add_rule(
        "no_nulls",
        lambda df: check_no_nulls(df, ["price", "market_cap", "volume"]),
    )
    runner.add_rule("positive_price", lambda df: check_positive_values(df))
    runner.add_rule("valid_timestamps", lambda df: check_valid_timestamps(df))
    
    return runner