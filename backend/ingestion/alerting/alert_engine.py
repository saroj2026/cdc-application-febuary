"""Alert engine for processing and delivering alerts."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_

from ingestion.database.models_db import AlertRuleModel, AlertHistoryModel, PipelineModel

logger = logging.getLogger(__name__)


class AlertEngine:
    """Advanced alerting with multiple channels."""
    
    def __init__(self, db_session: Session):
        """Initialize alert engine.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
    
    def check_alerts(
        self,
        pipeline_id: str,
        metrics: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Check if any alert rules are triggered.
        
        Args:
            pipeline_id: Pipeline ID
            metrics: Current metrics
            
        Returns:
            List of triggered alerts
        """
        try:
            # Get active alert rules for this pipeline
            rules = self.db_session.query(AlertRuleModel).filter(
                and_(
                    AlertRuleModel.pipeline_id == pipeline_id,
                    AlertRuleModel.enabled == True
                )
            ).all()
            
            triggered_alerts = []
            
            for rule in rules:
                if self._evaluate_rule(rule, metrics):
                    alert = self._create_alert(rule, metrics)
                    triggered_alerts.append(alert)
                    self._store_alert(alert)
            
            return triggered_alerts
            
        except Exception as e:
            logger.error(f"Error checking alerts: {e}", exc_info=True)
            return []
    
    def _evaluate_rule(
        self,
        rule: AlertRuleModel,
        metrics: Dict[str, Any]
    ) -> bool:
        """Evaluate if an alert rule should trigger.
        
        Args:
            rule: Alert rule
            metrics: Current metrics
            
        Returns:
            True if rule should trigger
        """
        try:
            metric_value = metrics.get(rule.metric)
            if metric_value is None:
                return False
            
            condition = rule.condition
            threshold = rule.threshold
            
            if condition == "greater_than":
                return metric_value > threshold
            elif condition == "less_than":
                return metric_value < threshold
            elif condition == "equals":
                return metric_value == threshold
            elif condition == "not_equals":
                return metric_value != threshold
            else:
                logger.warning(f"Unknown condition: {condition}")
                return False
                
        except Exception as e:
            logger.error(f"Error evaluating rule: {e}")
            return False
    
    def _create_alert(
        self,
        rule: AlertRuleModel,
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create an alert from a triggered rule.
        
        Args:
            rule: Alert rule
            metrics: Current metrics
            
        Returns:
            Alert dictionary
        """
        metric_value = metrics.get(rule.metric, "N/A")
        
        return {
            "rule_id": rule.id,
            "rule_name": rule.name,
            "pipeline_id": rule.pipeline_id,
            "severity": rule.severity,
            "metric": rule.metric,
            "metric_value": metric_value,
            "threshold": rule.threshold,
            "condition": rule.condition,
            "message": f"{rule.name}: {rule.metric} ({metric_value}) {rule.condition} {rule.threshold}",
            "channels": rule.channels or [],
            "triggered_at": datetime.utcnow().isoformat(),
            "status": "active"
        }
    
    def _store_alert(
        self,
        alert: Dict[str, Any]
    ) -> None:
        """Store alert in database.
        
        Args:
            alert: Alert dictionary
        """
        try:
            alert_record = AlertHistoryModel(
                rule_id=alert["rule_id"],
                pipeline_id=alert["pipeline_id"],
                triggered_at=datetime.utcnow(),
                status="active",
                message=alert["message"],
                metadata=alert
            )
            
            self.db_session.add(alert_record)
            self.db_session.commit()
            
            # Send alerts via configured channels
            self._send_alerts(alert)
            
        except Exception as e:
            logger.error(f"Error storing alert: {e}", exc_info=True)
            self.db_session.rollback()
    
    def _send_alerts(
        self,
        alert: Dict[str, Any]
    ) -> None:
        """Send alerts via configured channels.
        
        Args:
            alert: Alert dictionary
        """
        channels = alert.get("channels", [])
        
        for channel in channels:
            try:
                if channel == "email":
                    self._send_email_alert(alert)
                elif channel == "webhook":
                    self._send_webhook_alert(alert)
                elif channel == "slack":
                    self._send_slack_alert(alert)
                else:
                    logger.warning(f"Unknown alert channel: {channel}")
            except Exception as e:
                logger.error(f"Error sending alert via {channel}: {e}")
    
    def _send_email_alert(self, alert: Dict[str, Any]) -> None:
        """Send email alert (placeholder).
        
        Args:
            alert: Alert dictionary
        """
        # TODO: Implement email sending
        logger.info(f"Email alert: {alert['message']}")
    
    def _send_webhook_alert(self, alert: Dict[str, Any]) -> None:
        """Send webhook alert (placeholder).
        
        Args:
            alert: Alert dictionary
        """
        # TODO: Implement webhook sending
        logger.info(f"Webhook alert: {alert['message']}")
    
    def _send_slack_alert(self, alert: Dict[str, Any]) -> None:
        """Send Slack alert (placeholder).
        
        Args:
            alert: Alert dictionary
        """
        # TODO: Implement Slack sending
        logger.info(f"Slack alert: {alert['message']}")
    
    def get_active_alerts(
        self,
        pipeline_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get active alerts.
        
        Args:
            pipeline_id: Pipeline ID (optional, filter by pipeline)
            
        Returns:
            List of active alerts
        """
        try:
            query = self.db_session.query(AlertHistoryModel).filter(
                AlertHistoryModel.status == "active"
            )
            
            if pipeline_id:
                query = query.filter(AlertHistoryModel.pipeline_id == pipeline_id)
            
            alerts = query.order_by(AlertHistoryModel.triggered_at.desc()).all()
            
            return [
                {
                    "id": alert.id,
                    "rule_id": alert.rule_id,
                    "pipeline_id": alert.pipeline_id,
                    "triggered_at": alert.triggered_at.isoformat(),
                    "message": alert.message,
                    "metadata": alert.metadata
                }
                for alert in alerts
            ]
            
        except Exception as e:
            logger.error(f"Error getting active alerts: {e}", exc_info=True)
            return []


