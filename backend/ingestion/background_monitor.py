"""Background monitoring service for CDC pipelines."""

import logging
import threading
import time
from datetime import datetime
from typing import Optional

from ingestion.database.session import get_db
from ingestion.cdc_health_monitor import CDCHealthMonitor
from ingestion.kafka_connect_client import KafkaConnectClient
from ingestion.connection_service import ConnectionService
from ingestion.cdc_manager import CDCManager

logger = logging.getLogger(__name__)


class BackgroundCDCMonitor:
    """Background service that periodically monitors all CDC pipelines."""
    
    def __init__(
        self,
        kafka_connect_url: str = "http://72.61.233.209:8083",
        check_interval_seconds: int = 60  # Check every minute
    ):
        """Initialize background monitor.
        
        Args:
            kafka_connect_url: Kafka Connect URL (base URL only, without /connectors)
            check_interval_seconds: How often to check pipelines (default: 60 seconds)
        """
        # Normalize URL: remove any trailing /connectors to prevent double path
        kafka_connect_url = kafka_connect_url.rstrip('/connectors').rstrip('/')
        self.kafka_connect_url = kafka_connect_url
        self.check_interval = check_interval_seconds
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Initialize services
        self.kafka_client = KafkaConnectClient(base_url=kafka_connect_url)
        self.connection_service = ConnectionService()
        self.cdc_manager = CDCManager(kafka_connect_url=kafka_connect_url)
    
    def start(self):
        """Start the background monitoring service."""
        if self.running:
            logger.warning("Background monitor is already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"Background CDC monitor started (check interval: {self.check_interval}s)")
    
    def stop(self):
        """Stop the background monitoring service."""
        if not self.running:
            return
        
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Background CDC monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                self._check_all_pipelines()
            except Exception as e:
                logger.error(f"Error in background monitor loop: {e}", exc_info=True)
            
            # Sleep until next check
            time.sleep(self.check_interval)
    
    def _check_all_pipelines(self):
        """Check all running pipelines."""
        try:
            db = next(get_db())
            health_monitor = CDCHealthMonitor(
                kafka_client=self.kafka_client,
                connection_service=self.connection_service,
                db_session=db,
                kafka_connect_url=self.kafka_connect_url
            )
            
            results = health_monitor.monitor_all_pipelines()
            
            # Log summary
            healthy_count = sum(1 for r in results if r and r.get("status") == "healthy")
            unhealthy_count = sum(1 for r in results if r and r.get("status") == "unhealthy")
            recovery_results = [r.get("recovery_result") for r in results if r and r.get("recovery_result")]
            recovered_count = sum(1 for rr in recovery_results if rr and rr.get("success") is True)
            
            if unhealthy_count > 0 or recovered_count > 0:
                logger.info(
                    f"Background monitor check: {len(results)} pipelines, "
                    f"{healthy_count} healthy, {unhealthy_count} unhealthy, "
                    f"{recovered_count} recovered"
                )
            
            # Log details for unhealthy pipelines
            for result in results:
                if result.get("status") == "unhealthy":
                    pipeline_name = result.get("pipeline_name", "unknown")
                    lag_kb = result.get("lag_info", {}).get("lag_kb", 0)
                    issues = result.get("issues", [])
                    logger.warning(
                        f"Unhealthy pipeline: {pipeline_name} "
                        f"(lag: {lag_kb:.2f} KB, issues: {', '.join(issues)})"
                    )
            
            db.close()
            
        except Exception as e:
            logger.error(f"Error checking pipelines: {e}", exc_info=True)


# Global instance
_background_monitor: Optional[BackgroundCDCMonitor] = None


def start_background_monitor(
    kafka_connect_url: str = "http://72.61.233.209:8083",
    check_interval_seconds: int = 60
):
    """Start the global background monitor.
    
    Args:
        kafka_connect_url: Kafka Connect URL (base URL only, without /connectors)
        check_interval_seconds: Check interval in seconds
    """
    global _background_monitor
    if _background_monitor is None:
        # Normalize URL: remove any trailing /connectors to prevent double path
        normalized_url = kafka_connect_url.rstrip('/connectors').rstrip('/')
        _background_monitor = BackgroundCDCMonitor(
            kafka_connect_url=normalized_url,
            check_interval_seconds=check_interval_seconds
        )
        _background_monitor.start()
    return _background_monitor


def stop_background_monitor():
    """Stop the global background monitor."""
    global _background_monitor
    if _background_monitor:
        _background_monitor.stop()
        _background_monitor = None

