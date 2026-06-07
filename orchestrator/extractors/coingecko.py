"""
CoinGecko API extractor with built-in checkpoint filtering.
Deduplication happens HERE — rows older than watermark are filtered out
before returning the DataFrame.
"""

import logging
from typing import Dict, List, Optional

import polars as pl
import requests

from core.base import BaseExtractor
from core.state_manager import WatermarkManager
from core.exceptions import ExtractionError

logger = logging.getLogger(__name__)


class CoinGeckoExtractor(BaseExtractor):
    """
    Extracts 5-minute cryptocurrency data from CoinGecko.
    
    Implements the High-Water Mark pattern:
    1. Check watermark for each coin
    2. Fetch 24h rolling window from API
    3. FILTER: keep only rows with timestamp_ms > watermark
    4. Return clean DataFrame (no duplicates)
    """
    
    def __init__(
        self,
        base_url: str,
        coins: List[str],
        vs_currency: str = "usd",
        days: int = 1,
        timeout: int = 30,
        watermark_manager: Optional[WatermarkManager] = None,
    ):
        self.base_url = base_url
        self.coins = coins
        self.vs_currency = vs_currency
        self.days = days
        self.timeout = timeout
        self.watermark_manager = watermark_manager
    
    def extract(self, **kwargs) -> Dict[str, pl.DataFrame]:
        """
        Extract data for all coins with checkpoint filtering.
        
        Returns:
            Dict mapping coin_id -> filtered DataFrame
        """
        results = {}
        
        with requests.Session() as session:
            for coin_id in self.coins:
                try:
                    df = self._extract_single_coin(session, coin_id)
                    if not df.is_empty():
                        results[coin_id] = df
                        logger.info(
                            "Extracted %s: %d new rows",
                            coin_id, df.height,
                        )
                    else:
                        logger.info("No new data for %s", coin_id)
                except Exception as exc:
                    logger.error("Failed to extract %s: %s", coin_id, exc)
                    raise ExtractionError(f"Extraction failed for {coin_id}: {exc}") from exc
        
        return results
    
    def _extract_single_coin(
        self, session: requests.Session, coin_id: str
    ) -> pl.DataFrame:
        """
        Extract and filter data for a single coin.
        
        Steps:
        1. Get watermark checkpoint
        2. Fetch raw API data
        3. Flatten JSON to DataFrame
        4. Filter out rows already processed (dedup)
        """
        # Step 1: Get checkpoint
        checkpoint = 0
        if self.watermark_manager:
            checkpoint = self.watermark_manager.get_checkpoint(coin_id)
        
        if checkpoint > 0:
            logger.debug(
                "%s checkpoint: %s",
                coin_id,
                __import__("datetime").datetime.utcfromtimestamp(checkpoint / 1000),
            )
        
        # Step 2: Fetch from API
        raw_data = self._fetch_raw(session, coin_id)
        
        # Step 3: Flatten
        df = self._flatten_response(coin_id, raw_data)
        
        # Step 4: DEDUP — filter out already-processed rows
        if checkpoint > 0:
            before_count = df.height
            df = df.filter(pl.col("timestamp_ms") > checkpoint)
            skipped = before_count - df.height
            if skipped > 0:
                logger.info(
                    "  %s: %d new rows, %d skipped (checkpoint filter)",
                    coin_id, df.height, skipped,
                )
        
        return df
    
    def _fetch_raw(self, session: requests.Session, coin_id: str) -> dict:
        """Fetch raw JSON from CoinGecko API."""
        url = f"{self.base_url}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": self.vs_currency,
            "days": self.days,
        }
        
        resp = session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        
        # Validate response structure
        required_keys = ["prices", "market_caps", "total_volumes"]
        if not all(k in data for k in required_keys):
            raise ExtractionError(
                f"Unexpected API response for {coin_id}: missing {required_keys}"
            )
        
        return data
    
    def _flatten_response(self, coin_id: str, raw: dict) -> pl.DataFrame:
        """
        Convert CoinGecko nested arrays to flat Polars DataFrame.
        
        Input:
            {"prices": [[ts, val], ...],
             "market_caps": [[ts, val], ...],
             "total_volumes": [[ts, val], ...]}
        
        Output columns:
            coin_id, timestamp_ms, price, market_cap, volume
        """
        def array_to_df(arr: List, col_name: str) -> pl.DataFrame:
            return pl.DataFrame(
                arr,
                schema=["timestamp_ms", col_name],
                orient="row",
            ).with_columns(pl.col("timestamp_ms").cast(pl.Int64))
        
        df_prices = array_to_df(raw["prices"], "price")
        df_caps = array_to_df(raw["market_caps"], "market_cap")
        df_volumes = array_to_df(raw["total_volumes"], "volume")
        
        # Join all three on timestamp_ms
        df = (
            df_prices
            .join(df_caps, on="timestamp_ms", how="inner")
            .join(df_volumes, on="timestamp_ms", how="inner")
            .with_columns(pl.lit(coin_id).alias("coin_id"))
            .select(["coin_id", "timestamp_ms", "price", "market_cap", "volume"])
        )
        
        return df
    
    def get_new_watermarks(
        self, extracted_data: Dict[str, pl.DataFrame]
    ) -> Dict[str, int]:
        """
        Extract new watermark values from extracted data.
        
        Returns:
            Dict mapping coin_id -> max timestamp_ms
        """
        new_watermarks = {}
        for coin_id, df in extracted_data.items():
            if not df.is_empty():
                new_watermarks[coin_id] = df["timestamp_ms"].max()
        return new_watermarks