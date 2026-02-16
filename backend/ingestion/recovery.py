"""Error recovery and resilience for CDC pipelines."""

import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ingestion.cdc_manager import CDCManager
from ingestion.models import Pipeline, PipelineStatus

logger = logging.getLogger(__name__)


class RecoveryManager:
    """Automatic error recovery for failed pipelines."""
    
    def __init__(self, cdc_manager: CDCManager):
        """Initialize recovery manager.
        
        Args:
            cdc_manager: CDC Manager instance
        """
        self.cdc_manager = cdc_manager
        self.max_retries = 3
        self.retry_delay_seconds = 60
    
    def recover_failed_pipeline(
        self,
        pipeline_id: str
    ) -> Dict[str, Any]:
        """Attempt to recover a failed pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Recovery result
        """
        try:
            pipeline = self.cdc_manager.pipeline_store.get(pipeline_id)
            if not pipeline:
                return {
                    "success": False,
                    "error": f"Pipeline not found: {pipeline_id}"
                }
            
            if pipeline.status != PipelineStatus.ERROR:
                return {
                    "success": False,
                    "error": f"Pipeline is not in ERROR state: {pipeline.status}"
                }
            
            logger.info(f"Attempting to recover pipeline: {pipeline_id}")
            
            # Stop the pipeline first
            try:
                self.cdc_manager.stop_pipeline(pipeline_id)
                time.sleep(5)  # Wait for connectors to stop
            except Exception as e:
                logger.warning(f"Error stopping pipeline during recovery: {e}")
            
            # Restart the pipeline
            try:
                result = self.cdc_manager.start_pipeline(pipeline_id)
                return {
                    "success": True,
                    "message": "Pipeline recovered and restarted",
                    "result": result
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to restart pipeline: {str(e)}"
                }
                
        except Exception as e:
            logger.error(f"Error recovering pipeline: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def auto_recover_all_failed(
        self
    ) -> Dict[str, Any]:
        """Automatically recover all failed pipelines.
        
        Returns:
            Recovery summary
        """
        failed_pipelines = [
            p for p in self.cdc_manager.pipeline_store.values()
            if p.status == PipelineStatus.ERROR
        ]
        
        results = {
            "total_failed": len(failed_pipelines),
            "recovered": 0,
            "failed": 0,
            "details": []
        }
        
        for pipeline in failed_pipelines:
            result = self.recover_failed_pipeline(pipeline.id)
            if result.get("success"):
                results["recovered"] += 1
            else:
                results["failed"] += 1
            
            results["details"].append({
                "pipeline_id": pipeline.id,
                "pipeline_name": pipeline.name,
                "result": result
            })
        
        return results
    
    def check_and_recover_connectors(
        self,
        pipeline_id: str
    ) -> Dict[str, Any]:
        """Check connector health and recover if needed.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Recovery result
        """
        try:
            pipeline = self.cdc_manager.pipeline_store.get(pipeline_id)
            if not pipeline:
                return {
                    "success": False,
                    "error": f"Pipeline not found: {pipeline_id}"
                }
            
            from ingestion.monitoring import CDCMonitor
            monitor = CDCMonitor(self.cdc_manager.kafka_client)
            health = monitor.check_pipeline_health(pipeline)
            
            if health.get("overall_status") == "healthy":
                return {
                    "success": True,
                    "message": "Pipeline is healthy, no recovery needed",
                    "health": health
                }
            
            # Pipeline is unhealthy, attempt recovery
            logger.warning(f"Pipeline {pipeline_id} is unhealthy, attempting recovery")
            return self.recover_failed_pipeline(pipeline_id)
            
        except Exception as e:
            logger.error(f"Error checking and recovering connectors: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }


