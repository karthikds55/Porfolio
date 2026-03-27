"""Main ETL orchestrator for the Daily Ecommerce Orders pipeline."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from loguru import logger

from etl import config
from etl.extract import extract
from etl.load import load
from etl.transform import transform
from etl.utils.logger import setup_logger


def run_pipeline(source_file=None) -> Dict:
    """Execute the full Extract → Transform → Load pipeline.

    Args:
        source_file: Override path to the raw CSV.  Defaults to config.RAW_FILE.

    Returns:
        A summary dict with run stats and output locations.
    """
    setup_logger(log_dir=str(config.LOGS_DIR))
    start_ts = datetime.now(tz=timezone.utc)
    t0 = time.perf_counter()

    logger.info("=" * 65)
    logger.info("  Daily Ecommerce Orders – ETL Pipeline")
    logger.info(f"  Run started at: {start_ts.isoformat()}")
    logger.info("=" * 65)

    try:
        logger.info("── STEP 1/3  EXTRACT ──────────────────────────────────────")
        raw_df = extract(source_file or config.RAW_FILE)

        logger.info("── STEP 2/3  TRANSFORM ────────────────────────────────────")
        clean_df, rejected_df = transform(raw_df)

        logger.info("── STEP 3/3  LOAD ─────────────────────────────────────────")
        run_meta = {
            "run_start_utc": start_ts.isoformat(),
            "source_file": str(source_file or config.RAW_FILE),
            "raw_rows": len(raw_df),
        }
        outputs = load(clean_df, rejected_df, run_meta)

        elapsed = time.perf_counter() - t0
        summary = {
            "status": "SUCCESS",
            "run_start_utc": start_ts.isoformat(),
            "elapsed_seconds": round(elapsed, 3),
            "raw_rows": len(raw_df),
            "clean_rows": len(clean_df),
            "rejected_rows": len(rejected_df),
            "rejection_rate_pct": round(len(rejected_df) / len(raw_df) * 100, 2),
            "outputs": outputs,
        }

        logger.info("=" * 65)
        logger.info("  Pipeline completed SUCCESSFULLY")
        logger.info(f"  Raw rows     : {summary['raw_rows']:,}")
        logger.info(f"  Clean rows   : {summary['clean_rows']:,}")
        logger.info(f"  Rejected rows: {summary['rejected_rows']:,}  "
                    f"({summary['rejection_rate_pct']}%)")
        logger.info(f"  Elapsed      : {elapsed:.3f}s")
        logger.info("=" * 65)
        return summary

    except FileNotFoundError as exc:
        logger.error(f"[PIPELINE] Source file not found: {exc}")
        sys.exit(1)
    except ValueError as exc:
        logger.error(f"[PIPELINE] Validation error: {exc}")
        sys.exit(1)
    except Exception as exc:
        logger.exception(f"[PIPELINE] Unexpected error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
