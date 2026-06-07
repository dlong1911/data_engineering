"""Pipeline metrics tracking."""

import time
from contextlib import contextmanager
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects timing and row count metrics for pipeline steps."""
    
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
    
    def record(self, step: str, duration_seconds: float, rows: int = 0, **kwargs):
        """Record metrics for a pipeline step."""
        self.metrics[step] = {
            "duration_seconds": round(duration_seconds, 3),
            "rows": rows,
            **kwargs,
        }
    
    def summary(self) -> str:
        """Generate a summary string of all metrics."""
        lines = ["Pipeline Metrics Summary:"]
        for step, metrics in self.metrics.items():
            parts = [f"  {step}:"]
            if "duration_seconds" in metrics:
                parts.append(f"{metrics['duration_seconds']}s")
            if "rows" in metrics:
                parts.append(f"{metrics['rows']} rows")
            lines.append(" | ".join(parts))
        return "\n".join(lines)


@contextmanager
def timed_operation(collector: MetricsCollector, step_name: str):
    """Context manager to time an operation and record metrics."""
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        collector.record(step_name, duration_seconds=duration)
        logger.info("⏱️  %s completed in %.2fs", step_name, duration)