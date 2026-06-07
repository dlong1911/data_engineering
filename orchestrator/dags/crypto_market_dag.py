"""
Airflow DAG: Daily Incremental 5-Minute Crypto Market Load.

Pattern: High-Water Mark with Control Table
  - Extract: API → filter > watermark → clean data
  - Bronze: Append clean data with metadata
  - Silver: Read today's Bronze partition → simple append

No duplicates. No MERGE. No full table scans.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import yaml

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Import framework components
import sys
sys.path.append("/opt/airflow")

from core.state_manager import WatermarkManager
from core.exceptions import DataQualityError, ExtractionError, WriteError
from extractors.coingecko import CoinGeckoExtractor
from transformers.bronze_enricher import BronzeEnricher
from quality.dq_runner import create_bronze_dq_runner
from writers.delta_writer import DeltaWriter
from utils.metrics import MetricsCollector, timed_operation

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
CONFIG_PATH = Path(__file__).parent.parent / "configs" / "pipeline_config.yaml"
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

PIPELINE_NAME = config["pipeline"]["name"]
API_CONFIG = config["api"]
STORAGE_CONFIG = config["storage"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_market_high_watermark",
    default_args=default_args,
    description="Checkpoint-based crypto ingestion (Bronze → Silver)",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["crypto", "deltalake", "checkpoint", "production"],
) as dag:

    @task(task_id="extract_and_load_bronze")
    def extract_and_load_bronze(**context) -> dict:
        """
        Task 1: Extract with checkpoint → enrich → DQ → write Bronze → update watermarks.
        
        The deduplication happens inside CoinGeckoExtractor.extract()
        via the High-Water Mark filter.
        """
        metrics = MetricsCollector()
        logical_date = context["logical_date"]
        ds = context["ds"]
        run_id = context["run_id"]
        base_path = STORAGE_CONFIG["base_path"]

        logger.info("=" * 60)
        logger.info("🚀 Bronze Ingestion | ds=%s | run_id=%s", ds, run_id)

        # --- Initialize components ---
        wm_path = f"{base_path}/{STORAGE_CONFIG['watermark_table']}"
        watermark_mgr = WatermarkManager(wm_path, PIPELINE_NAME)

        extractor = CoinGeckoExtractor(
            base_url=API_CONFIG["base_url"],
            coins=API_CONFIG["coins"],
            vs_currency=API_CONFIG["params"]["vs_currency"],
            days=API_CONFIG["params"]["days"],
            timeout=API_CONFIG["timeout_seconds"],
            watermark_manager=watermark_mgr,
        )

        enricher = BronzeEnricher()
        dq_runner = create_bronze_dq_runner()
        writer = DeltaWriter(base_path)

        try:
            # --- Extract with checkpoint filtering (DEDUP HERE) ---
            with timed_operation(metrics, "extraction"):
                extracted_data = extractor.extract()
            
            if not extracted_data:
                logger.warning("No new data for any coin. Skipping.")
                return {"status": "no_data", "processing_date": ds}

            # --- Enrich each coin's DataFrame ---
            enriched_dfs = []
            for coin_id, df in extracted_data.items():
                df = enricher.transform(df, processing_date=ds, airflow_run_id=run_id)
                enriched_dfs.append(df)

                # Rate limiting between coins
                time.sleep(API_CONFIG["rate_limit_seconds"])

            # --- Combine & DQ check ---
            bronze_df = pl.concat(enriched_dfs, how="vertical")
            
            with timed_operation(metrics, "quality_check"):
                dq_runner.run(bronze_df, stage="bronze")
            
            metrics.record("bronze_rows", rows=bronze_df.height)

            # --- Write Bronze ---
            with timed_operation(metrics, "bronze_write"):
                writer.append_bronze(bronze_df)

            # --- Update watermarks ---
            new_watermarks = extractor.get_new_watermarks(extracted_data)
            watermark_mgr.update_watermarks(new_watermarks)

            logger.info(metrics.summary())
            
            return {
                "status": "success",
                "bronze_rows": bronze_df.height,
                "coins_processed": list(extracted_data.keys()),
                "processing_date": ds,
            }

        except (ExtractionError, DataQualityError, WriteError) as exc:
            logger.error("❌ Bronze task failed: %s", exc)
            raise

    @task(task_id="load_silver")
    def load_silver(bronze_result: dict) -> dict:
        """
        Task 2: Read today's Bronze partition → DQ → append to Silver.
        
        Because Bronze is already deduplicated at extraction,
        this is a simple partition scan + append. No MERGE needed.
        """
        if bronze_result.get("status") == "no_data":
            logger.info("No new Bronze data, skipping Silver load.")
            return {"status": "skipped"}

        metrics = MetricsCollector()
        processing_date = bronze_result["processing_date"]
        base_path = STORAGE_CONFIG["base_path"]

        logger.info("=" * 60)
        logger.info("🔄 Silver Load | ds=%s", processing_date)

        dq_runner = create_bronze_dq_runner()
        writer = DeltaWriter(base_path)

        try:
            # --- Read only today's Bronze partition ---
            bronze_path = f"{base_path}/{STORAGE_CONFIG['bronze_table']}"
        except:
            pass
        