import json
import logging
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import psutil

from open_auto_loader import OpenAutoLoader

# Configuration
DATA_DIR = Path("data")  # Where your big CSV/TXT files are
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# Setup logging to see progress
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_performance_test():
    # 1. Setup Environment
    root = Path("./perf_run_metadata")
    target = root / "delta_table"
    checkpoint = root / "checkpoint"
    schema_path = root / "schema_root"

    # Ensure fresh run for performance accuracy (clean metadata)
    import shutil

    if root.exists():
        shutil.rmtree(root)

    for p in [target, checkpoint, schema_path]:
        p.mkdir(parents=True, exist_ok=True)

    # 2. Identify Test File
    # We look for .csv or .txt in the data/ folder
    test_files = list(DATA_DIR.glob("*.csv")) + list(DATA_DIR.glob("*.txt"))
    if not test_files:
        logger.error(
            f"No data files found in {DATA_DIR}. Please place your big file there."
        )
        return

    test_file = test_files[0]
    file_size_gb = test_file.stat().st_size / (1024**3)
    # Quick row count estimate via Polars (fast for local files)
    row_count = (
        pl.scan_csv(test_file, ignore_errors=True).select(pl.len()).collect().item()
    )

    logger.info(f"🚀 Starting Performance Test on: {test_file.name}")
    logger.info(f"📊 Size: {file_size_gb:.2f} GB | Rows: {row_count:,}")

    # 3. Resource Tracking Baseline
    process = psutil.Process()
    start_time = time.perf_counter()
    initial_mem = process.memory_info().rss / (1024**2)  # MB

    # 4. Execute Load
    try:
        loader = OpenAutoLoader(
            source=str(DATA_DIR),
            target=str(target),
            check_point=str(checkpoint),
            schema_path=str(schema_path),
            extension=test_file.suffix.replace(".", ""),  # Dynamically use csv or txt
        )

        # We run with a unique batch ID
        loader.run(batch_id=f"perf_test_{datetime.now().strftime('%H%M%S')}")

    except Exception as e:
        logger.error(f"❌ Performance test failed: {e}")
        return

    # 5. Metric Collection
    end_time = time.perf_counter()
    duration = end_time - start_time
    peak_mem = process.memory_info().rss / (1024**2)  # MB

    # 6. Generate Report
    report = {
        "metadata": {
            "test_date": datetime.now().isoformat(),
            "file_name": test_file.name,
            "file_size_gb": round(file_size_gb, 4),
        },
        "performance": {
            "total_seconds": round(duration, 2),
            "rows_per_second": round(row_count / duration, 2),
            "mb_per_second": round((file_size_gb * 1024) / duration, 2),
            "peak_ram_usage_mb": round(peak_mem, 2),
            "ram_increase_mb": round(peak_mem - initial_mem, 2),
        },
    }

    # Save to reports folder as JSON
    report_file = (
        REPORTS_DIR
        / f"perf_{test_file.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    logger.info("✅ Performance test complete!")
    logger.info(
        f"⏱️ Duration: {duration:.2f}s | 📈 Speed: {report['performance']['rows_per_second']:,} rows/s"
    )
    logger.info(f"💾 Report saved to: {report_file}")


if __name__ == "__main__":
    run_performance_test()
