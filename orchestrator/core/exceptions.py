"""Custom exceptions for the ETL framework."""

class ETLError(Exception):
    """Base exception for ETL pipeline errors."""
    pass


class ExtractionError(ETLError):
    """Raised when data extraction fails."""
    pass


class TransformationError(ETLError):
    """Raised when data transformation fails."""
    pass


class DataQualityError(ETLError):
    """Raised when data quality checks fail."""
    
    def __init__(self, message: str, failures: dict = None):
        super().__init__(message)
        self.failures = failures or {}


class WatermarkError(ETLError):
    """Raised when watermark operations fail."""
    pass


class WriteError(ETLError):
    """Raised when data writing fails."""
    pass