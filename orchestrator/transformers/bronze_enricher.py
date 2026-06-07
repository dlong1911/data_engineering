"""
Bronze layer enricher.
Adds metadata columns: _ingested_at, _processing_date, _airflow_run_id
"""

import logging
from datetime import datetime, timezone

import polars as pl

from core.base import BaseTransformer

logger = logging.getLogger(__name__)


class BronzeEnricher(BaseTransformer):
    """
    Adds metadata audit columns to extracted DataFrames.
    
    Columns added:
      - _ingested_at: UTC timestamp of ingestion
      - _processing_date: Airflow ds (YYYY-MM-DD)
      - _airflow_run_id: Airflow run identifier
    """
    
    def transform(
        self,
        df: pl.DataFrame,
        processing_date: str = None,
        airflow_run_id: str = None,
        **kwargs,
    ) -> pl.DataFrame:
        """
        Enrich DataFrame with metadata columns.
        
        Args:
            df: Clean DataFrame (already deduplicated)
            processing_date: Airflow logical date (YYYY-MM-DD)
            airflow_run_id: Airflow run_id for audit
        
        Returns:
            Enriched DataFrame with 3 new columns
        """
        ingested_at = datetime.now(timezone.utc)
        
        enrichments = []
        
        if processing_date:
            enrichments.append(
                pl.lit(processing_date).alias("_processing_date")
            )
        
        if airflow_run_id:
            enrichments.append(
                pl.lit(airflow_run_id).alias("_airflow_run_id")
            )
        
        enrichments.append(
            pl.lit(ingested_at).alias("_ingested_at")
        )
        
        if enrichments:
            df = df.with_columns(enrichments)
            logger.debug("Added %d metadata columns", len(enrichments))
        
        return df