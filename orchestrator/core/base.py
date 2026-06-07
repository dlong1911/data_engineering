"""Base classes for ETL components."""

from abc import ABC, abstractmethod
from typing import Any, Dict

import polars as pl


class BaseExtractor(ABC):
    """Abstract base for data extractors."""
    
    @abstractmethod
    def extract(self, **kwargs) -> pl.DataFrame:
        """Extract data from source."""
        pass


class BaseTransformer(ABC):
    """Abstract base for data transformers."""
    
    @abstractmethod
    def transform(self, df: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """Transform DataFrame."""
        pass


class BaseWriter(ABC):
    """Abstract base for data writers."""
    
    @abstractmethod
    def write(self, df: pl.DataFrame, **kwargs) -> Dict[str, Any]:
        """Write DataFrame to target."""
        pass


class BaseQualityChecker(ABC):
    """Abstract base for data quality checkers."""
    
    @abstractmethod
    def check(self, df: pl.DataFrame, **kwargs) -> bool:
        """Run quality checks on DataFrame."""
        pass