"""
Delta Lake writer.
Handles Bronze append, Silver append, and control table updates.
"""

import logging
from typing import Any, Dict, Optional

import polars as pl
from deltalake import write_deltalake

from core.base import BaseWriter
from core.exceptions import WriteError

logger = logging.getLogger(__name__)


class DeltaWriter(BaseWriter):
    """Writes Polars DataFrames to Delta Lake tables."""
    
    def __init__(self, base_path: str):
        self.base_path = base_path
    
    def write(
        self,
        df: pl.DataFrame,
        table_path: str,
        mode: str = "append",
        partition_by: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Write DataFrame to Delta table.
        
        Args:
            df: Polars DataFrame to write
            table_path: Relative path from base_path
            mode: Write mode ("append", "overwrite", etc.)
            partition_by: List of partition columns
        
        Returns:
            Dict with write metadata
        """
        full_path = f"{self.base_path}/{table_path}"
        
        try:
            write_deltalake(
                full_path,
                df.to_arrow(),
                mode=mode,
                partition_by=partition_by,
            )
            
            logger.info(
                "✅ Written %d rows to %s (mode=%s, partitions=%s)",
                df.height, table_path, mode, partition_by,
            )
            
            return {
                "path": full_path,
                "rows_written": df.height,
                "mode": mode,
                "partitions": partition_by,
            }
            
        except Exception as exc:
            raise WriteError(f"Failed to write to {table_path}: {exc}") from exc
    
    def append_bronze(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Append to Bronze table, partitioned by processing date."""
        return self.write(
            df,
            "bronze/crypto_prices",
            mode="append",
            partition_by=["_processing_date"],
        )
    
    def append_silver(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Append to Silver fact table, partitioned by coin_id."""
        return self.write(
            df,
            "silver/fact_crypto_markets",
            mode="append",
            partition_by=["coin_id"],
        )