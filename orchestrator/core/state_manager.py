"""
High-Water Mark / Checkpoint management.
Reads and writes watermark state to a dedicated Delta table.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import polars as pl
from deltalake import write_deltalake

from core.exceptions import WatermarkError

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Manages per-entity watermarks in a Delta control table.
    
    Schema:
      - pipeline_name: String (e.g., "coingecko_5min_ingest")
      - entity_id: String (e.g., "bitcoin")
      - last_processed_timestamp: Int64 (Unix milliseconds)
    """
    
    def __init__(self, watermark_path: str, pipeline_name: str):
        self.watermark_path = watermark_path
        self.pipeline_name = pipeline_name
    
    def load_watermarks(self) -> Dict[str, int]:
        """
        Load current watermarks from Delta table.
        
        Returns:
            Dict mapping entity_id -> last_processed_timestamp (ms)
            Empty dict if no watermarks exist yet.
        """
        if not Path(self.watermark_path).exists():
            logger.info("No watermark table yet — starting fresh")
            return {}
        
        try:
            df = pl.read_delta(self.watermark_path).filter(
                pl.col("pipeline_name") == self.pipeline_name
            )
            
            watermarks = {
                row["entity_id"]: row["last_processed_timestamp"]
                for row in df.iter_rows(named=True)
            }
            
            logger.info("Loaded %d watermarks from control table", len(watermarks))
            return watermarks
            
        except Exception as exc:
            logger.warning("Failed to read watermarks (starting fresh): %s", exc)
            return {}
    
    def get_checkpoint(self, entity_id: str) -> int:
        """
        Get checkpoint for a specific entity.
        
        Returns:
            Timestamp in milliseconds, or 0 if no checkpoint exists.
        """
        watermarks = self.load_watermarks()
        return watermarks.get(entity_id, 0)
    
    def update_watermarks(
        self,
        new_watermarks: Dict[str, int],
        existing_watermarks: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Merge new watermarks with existing and overwrite control table.
        
        Strategy:
          - Entities with new data: use new max timestamp
          - Entities without new data: keep existing value
          - New entities: add to table
        
        Args:
            new_watermarks: Dict mapping entity_id -> max timestamp from this run
            existing_watermarks: Dict of existing watermarks (loaded earlier)
        """
        if existing_watermarks is None:
            existing_watermarks = self.load_watermarks()
        
        # Start with existing, keep only entities NOT updated this run
        merged = {
            entity: ts
            for entity, ts in existing_watermarks.items()
            if entity not in new_watermarks
        }
        
        # Override with new values
        merged.update(new_watermarks)
        
        # Build DataFrame
        watermark_df = pl.DataFrame(
            [
                {
                    "pipeline_name": self.pipeline_name,
                    "entity_id": entity,
                    "last_processed_timestamp": ts,
                }
                for entity, ts in merged.items()
            ],
            schema={
                "pipeline_name": pl.Utf8,
                "entity_id": pl.Utf8,
                "last_processed_timestamp": pl.Int64,
            },
        )
        
        # Atomically overwrite control table
        try:
            write_deltalake(
                self.watermark_path,
                watermark_df.to_arrow(),
                mode="overwrite",
            )
            logger.info(
                "Control table updated: %d entities tracked",
                watermark_df.height,
            )
        except Exception as exc:
            raise WatermarkError(f"Failed to update watermarks: {exc}") from exc
        