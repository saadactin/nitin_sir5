"""
Advanced Alerting System - Phase 2
Enhanced alerting with multiple channels, escalation, and grouping
"""

import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
from utils.email_service import email_service
from realtime_monitor import broadcast_alert

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    HIGH = "high"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Alert notification channels"""
    EMAIL = "email"
    WEBSOCKET = "websocket"
    SLACK = "slack"
    WEBHOOK = "webhook"
    LOG = "log"


@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    condition: Callable  # Function that returns True if alert should trigger
    severity: AlertSeverity
    channels: List[AlertChannel]
    cooldown_minutes: int = 60  # Don't send same alert more than once per cooldown period
    escalation_minutes: int = 0  # Escalate if not resolved
    group_key: Optional[str] = None  # Group related alerts


@dataclass
class Alert:
    """Alert instance"""
    rule_name: str
    severity: AlertSeverity
    message: str
    details: Dict
    source_name: Optional[str] = None
    table_name: Optional[str] = None
    timestamp: datetime = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class AdvancedAlerting:
    """Advanced alerting system with multiple channels and escalation"""
    
    def __init__(self):
        self.rules: Dict[str, AlertRule] = {}
        self.alert_history: List[Alert] = []
        self.last_alert_time: Dict[str, datetime] = {}  # Track last alert time per rule
        self.alert_groups: Dict[str, List[Alert]] = defaultdict(list)
        self.escalation_timers: Dict[str, datetime] = {}
    
    def register_rule(self, rule: AlertRule):
        """Register an alert rule"""
        self.rules[rule.name] = rule
        logger.info(f"Registered alert rule: {rule.name}")
    
    def check_and_trigger(self, context: Dict) -> List[Alert]:
        """Check all rules and trigger alerts if conditions are met"""
        triggered_alerts = []
        
        for rule_name, rule in self.rules.items():
            try:
                # Check if condition is met
                if rule.condition(context):
                    # Check cooldown
                    last_alert = self.last_alert_time.get(rule_name)
                    if last_alert:
                        time_since_last = datetime.now() - last_alert
                        if time_since_last.total_seconds() < (rule.cooldown_minutes * 60):
                            continue  # Still in cooldown
                    
                    # Create alert
                    alert = Alert(
                        rule_name=rule_name,
                        severity=rule.severity,
                        message=context.get('message', f"Alert: {rule_name}"),
                        details=context.get('details', {}),
                        source_name=context.get('source_name'),
                        table_name=context.get('table_name')
                    )
                    
                    # Send alert through configured channels
                    self._send_alert(alert, rule)
                    
                    # Track alert
                    triggered_alerts.append(alert)
                    self.alert_history.append(alert)
                    self.last_alert_time[rule_name] = datetime.now()
                    
                    # Group alerts if group_key specified
                    if rule.group_key:
                        group_key = context.get(rule.group_key, 'default')
                        self.alert_groups[group_key].append(alert)
                    
                    # Set escalation timer if configured
                    if rule.escalation_minutes > 0:
                        self.escalation_timers[alert.rule_name] = (
                            datetime.now() + timedelta(minutes=rule.escalation_minutes)
                        )
            
            except Exception as e:
                logger.error(f"Error checking alert rule {rule_name}: {e}")
        
        return triggered_alerts
    
    def _send_alert(self, alert: Alert, rule: AlertRule):
        """Send alert through configured channels"""
        for channel in rule.channels:
            try:
                if channel == AlertChannel.EMAIL:
                    self._send_email_alert(alert, rule)
                elif channel == AlertChannel.WEBSOCKET:
                    self._send_websocket_alert(alert)
                elif channel == AlertChannel.SLACK:
                    self._send_slack_alert(alert)
                elif channel == AlertChannel.WEBHOOK:
                    self._send_webhook_alert(alert)
                elif channel == AlertChannel.LOG:
                    self._send_log_alert(alert, rule)
            except Exception as e:
                logger.error(f"Error sending alert via {channel}: {e}")
    
    def _send_email_alert(self, alert: Alert, rule: AlertRule):
        """Send alert via email"""
        try:
            severity_map = {
                AlertSeverity.CRITICAL: "CRITICAL",
                AlertSeverity.HIGH: "HIGH",
                AlertSeverity.WARNING: "WARNING",
                AlertSeverity.INFO: "INFO"
            }
            
            subject = f"[{severity_map[alert.severity]}] {alert.message}"
            
            # Use email service
            if alert.severity in [AlertSeverity.CRITICAL, AlertSeverity.HIGH]:
                email_service.notify_system_error(
                    title=alert.message,
                    details=str(alert.details)
                )
        except Exception as e:
            logger.error(f"Error sending email alert: {e}")
    
    def _send_websocket_alert(self, alert: Alert):
        """Send alert via WebSocket"""
        broadcast_alert(
            alert_type=alert.rule_name,
            severity=alert.severity.value,
            message=alert.message,
            details=alert.details
        )
    
    def _send_slack_alert(self, alert: Alert):
        """Send alert via Slack (if configured)"""
        # Placeholder for Slack integration
        logger.info(f"Slack alert: {alert.message}")
    
    def _send_webhook_alert(self, alert: Alert):
        """Send alert via webhook (if configured)"""
        # Placeholder for webhook integration
        logger.info(f"Webhook alert: {alert.message}")
    
    def _send_log_alert(self, alert: Alert, rule: AlertRule):
        """Send alert via logging"""
        log_level = {
            AlertSeverity.CRITICAL: logging.CRITICAL,
            AlertSeverity.HIGH: logging.ERROR,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.INFO: logging.INFO
        }.get(alert.severity, logging.INFO)
        
        logger.log(log_level, f"ALERT [{alert.rule_name}]: {alert.message}")
    
    def resolve_alert(self, rule_name: str, source_name: str = None, table_name: str = None):
        """Mark alert as resolved"""
        for alert in self.alert_history:
            if (alert.rule_name == rule_name and
                not alert.resolved and
                (not source_name or alert.source_name == source_name) and
                (not table_name or alert.table_name == table_name)):
                alert.resolved = True
                alert.resolved_at = datetime.now()
                logger.info(f"Alert resolved: {rule_name}")
    
    def get_active_alerts(
        self,
        severity: AlertSeverity = None,
        source_name: str = None
    ) -> List[Alert]:
        """Get active (unresolved) alerts"""
        alerts = [a for a in self.alert_history if not a.resolved]
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        if source_name:
            alerts = [a for a in alerts if a.source_name == source_name]
        
        return alerts
    
    def check_escalations(self):
        """Check and escalate alerts that haven't been resolved"""
        now = datetime.now()
        for rule_name, escalate_at in list(self.escalation_timers.items()):
            if now >= escalate_at:
                # Find unresolved alerts for this rule
                unresolved = [a for a in self.alert_history
                             if a.rule_name == rule_name and not a.resolved]
                
                if unresolved:
                    # Escalate
                    rule = self.rules.get(rule_name)
                    if rule:
                        # Increase severity and resend
                        for alert in unresolved:
                            if alert.severity == AlertSeverity.WARNING:
                                alert.severity = AlertSeverity.HIGH
                            elif alert.severity == AlertSeverity.HIGH:
                                alert.severity = AlertSeverity.CRITICAL
                            
                            self._send_alert(alert, rule)
                            logger.warning(f"Escalated alert: {rule_name}")
                
                # Remove escalation timer
                del self.escalation_timers[rule_name]


# Global alerting instance
advanced_alerting = AdvancedAlerting()

# Register default alert rules
def register_default_alert_rules():
    """Register default alert rules"""
    from data_integrity import ValidationResult
    
    # Validation failure rule
    def validation_failure_condition(context):
        result = context.get('validation_result')
        if isinstance(result, ValidationResult):
            return not result.success
        return False
    
    advanced_alerting.register_rule(AlertRule(
        name="validation_failure",
        condition=validation_failure_condition,
        severity=AlertSeverity.HIGH,
        channels=[AlertChannel.EMAIL, AlertChannel.WEBSOCKET],
        cooldown_minutes=60,
        group_key="source_name"
    ))
    
    # Sync gap rule
    def sync_gap_condition(context):
        gap = context.get('gap')
        if gap:
            return gap.get('severity') in ['high', 'critical']
        return False
    
    advanced_alerting.register_rule(AlertRule(
        name="sync_gap",
        condition=sync_gap_condition,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.EMAIL, AlertChannel.WEBSOCKET],
        cooldown_minutes=120,
        escalation_minutes=240
    ))
    
    # Performance degradation rule
    def performance_degradation_condition(context):
        metrics = context.get('performance_metrics')
        if metrics:
            duration = metrics.get('duration_ms', 0)
            return duration > 30000  # More than 30 seconds
        return False
    
    advanced_alerting.register_rule(AlertRule(
        name="performance_degradation",
        condition=performance_degradation_condition,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.WEBSOCKET, AlertChannel.LOG],
        cooldown_minutes=30
    ))
    
    # Data quality degradation rule
    def quality_degradation_condition(context):
        score = context.get('quality_score')
        if score:
            overall = score.get('overall_score', 100)
            return overall < 70  # Below 70%
        return False
    
    advanced_alerting.register_rule(AlertRule(
        name="quality_degradation",
        condition=quality_degradation_condition,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.EMAIL, AlertChannel.WEBSOCKET],
        cooldown_minutes=180,
        escalation_minutes=360
    ))


# Register default rules on import
register_default_alert_rules()

