"""
Puffy Analytics Production Monitoring System
=============================================
Monitors data pipeline health and alerts on anomalies.

This system monitors:
1. Data freshness and completeness
2. Revenue and conversion metrics
3. Traffic source health
4. Schema stability
5. Processing anomalies

Author: Data Infrastructure Team
Date: 2025
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class Alert:
    """Represents a monitoring alert."""
    name: str
    severity: AlertSeverity
    message: str
    metric_name: str
    current_value: float
    threshold: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'severity': self.severity.value,
            'message': self.message,
            'metric_name': self.metric_name,
            'current_value': self.current_value,
            'threshold': self.threshold,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class MetricResult:
    """Result of a metric check."""
    metric_name: str
    current_value: float
    historical_avg: Optional[float] = None
    historical_std: Optional[float] = None
    is_anomaly: bool = False
    z_score: Optional[float] = None


class DataMonitor:
    """
    Production monitoring system for Puffy analytics pipeline.
    
    Monitors daily data quality, business metrics, and pipeline health.
    """
    
    # Threshold configurations (tune based on historical patterns)
    THRESHOLDS = {
        # Absolute thresholds
        'min_daily_events': 2000,
        'max_daily_events': 8000,
        'min_daily_conversions': 10,
        'min_daily_revenue': 10000,
        'max_null_client_rate': 0.05,
        'max_duplicate_transactions': 0,
        
        # Standard deviation thresholds for anomaly detection
        'revenue_std_threshold': 2.5,  # Flag if >2.5 std from mean
        'conversion_std_threshold': 2.5,
        'traffic_std_threshold': 3.0,
        'aov_std_threshold': 2.0,
    }
    
    def __init__(self, historical_data: Optional[pd.DataFrame] = None):
        """
        Initialize monitor with optional historical data for baseline.
        
        Args:
            historical_data: DataFrame with columns [date, events, conversions, revenue]
        """
        self.historical_data = historical_data
        self.alerts: List[Alert] = []
        self.metrics: Dict[str, MetricResult] = {}
        
        # Calculate historical baselines if data provided
        if historical_data is not None:
            self._calculate_baselines()
    
    def _calculate_baselines(self) -> None:
        """Calculate historical baselines for anomaly detection."""
        self.baselines = {}
        
        if self.historical_data is None or len(self.historical_data) < 7:
            logger.warning("Insufficient historical data for baseline calculation")
            return
        
        for metric in ['events', 'conversions', 'revenue', 'aov']:
            if metric in self.historical_data.columns:
                self.baselines[metric] = {
                    'mean': self.historical_data[metric].mean(),
                    'std': self.historical_data[metric].std(),
                    'min': self.historical_data[metric].min(),
                    'max': self.historical_data[metric].max()
                }
    
    def check_all(self, today_data: pd.DataFrame, sessions_data: pd.DataFrame, 
                  conversions_data: pd.DataFrame) -> List[Alert]:
        """
        Run all monitoring checks.
        
        Args:
            today_data: Raw events for today
            sessions_data: Sessionized data for today
            conversions_data: Conversion records for today
            
        Returns:
            List of alerts generated
        """
        self.alerts = []
        
        # Data freshness and completeness
        self._check_event_volume(today_data)
        self._check_null_client_ids(today_data)
        self._check_schema_consistency(today_data)
        
        # Business metrics
        self._check_revenue(conversions_data)
        self._check_conversions(conversions_data)
        self._check_average_order_value(conversions_data)
        
        # Data integrity
        self._check_duplicate_transactions(conversions_data)
        self._check_conversion_funnel(sessions_data)
        
        # Traffic health
        self._check_traffic_sources(sessions_data)
        
        return self.alerts
    
    def _add_alert(self, name: str, severity: AlertSeverity, message: str,
                   metric_name: str, current_value: float, threshold: float) -> None:
        """Add an alert to the list."""
        alert = Alert(
            name=name,
            severity=severity,
            message=message,
            metric_name=metric_name,
            current_value=current_value,
            threshold=threshold
        )
        self.alerts.append(alert)
        logger.warning(f"ALERT [{severity.value}]: {message}")
    
    # =========================================================================
    # VOLUME AND COMPLETENESS CHECKS
    # =========================================================================
    
    def _check_event_volume(self, df: pd.DataFrame) -> None:
        """Check that event volume is within expected range."""
        event_count = len(df)
        
        self.metrics['event_count'] = MetricResult(
            metric_name='daily_event_count',
            current_value=event_count
        )
        
        if event_count < self.THRESHOLDS['min_daily_events']:
            self._add_alert(
                name="Low Event Volume",
                severity=AlertSeverity.CRITICAL,
                message=f"Daily events ({event_count:,}) below minimum ({self.THRESHOLDS['min_daily_events']:,})",
                metric_name='daily_event_count',
                current_value=event_count,
                threshold=self.THRESHOLDS['min_daily_events']
            )
        
        if event_count > self.THRESHOLDS['max_daily_events']:
            self._add_alert(
                name="High Event Volume",
                severity=AlertSeverity.WARNING,
                message=f"Daily events ({event_count:,}) above maximum ({self.THRESHOLDS['max_daily_events']:,})",
                metric_name='daily_event_count',
                current_value=event_count,
                threshold=self.THRESHOLDS['max_daily_events']
            )
    
    def _check_null_client_ids(self, df: pd.DataFrame) -> None:
        """Check NULL client_id rate."""
        client_col = 'clientId' if 'clientId' in df.columns else 'client_id'
        null_rate = (df[client_col].isna() | (df[client_col] == '')).mean()
        
        self.metrics['null_client_rate'] = MetricResult(
            metric_name='null_client_rate',
            current_value=null_rate
        )
        
        if null_rate > self.THRESHOLDS['max_null_client_rate']:
            self._add_alert(
                name="High NULL Client ID Rate",
                severity=AlertSeverity.CRITICAL,
                message=f"NULL client_id rate ({null_rate:.1%}) exceeds threshold ({self.THRESHOLDS['max_null_client_rate']:.1%})",
                metric_name='null_client_rate',
                current_value=null_rate,
                threshold=self.THRESHOLDS['max_null_client_rate']
            )
    
    def _check_schema_consistency(self, df: pd.DataFrame) -> None:
        """Check for expected columns."""
        expected = {'client_id', 'page_url', 'referrer', 'timestamp', 'event_name', 'event_data', 'user_agent'}
        actual = set(df.columns)
        
        # Handle clientId variant
        if 'clientId' in actual:
            actual.remove('clientId')
            actual.add('client_id')
        
        missing = expected - actual
        
        if missing:
            self._add_alert(
                name="Schema Change Detected",
                severity=AlertSeverity.CRITICAL,
                message=f"Missing columns: {missing}",
                metric_name='schema_columns',
                current_value=len(actual),
                threshold=len(expected)
            )
    
    # =========================================================================
    # BUSINESS METRIC CHECKS
    # =========================================================================
    
    def _check_revenue(self, conversions: pd.DataFrame) -> None:
        """Check daily revenue against thresholds and historical baseline."""
        revenue = conversions['revenue'].sum()
        
        result = MetricResult(
            metric_name='daily_revenue',
            current_value=revenue
        )
        
        # Absolute threshold check
        if revenue < self.THRESHOLDS['min_daily_revenue']:
            self._add_alert(
                name="Low Revenue",
                severity=AlertSeverity.CRITICAL,
                message=f"Daily revenue (${revenue:,.0f}) below minimum (${self.THRESHOLDS['min_daily_revenue']:,.0f})",
                metric_name='daily_revenue',
                current_value=revenue,
                threshold=self.THRESHOLDS['min_daily_revenue']
            )
        
        # Statistical anomaly check
        if hasattr(self, 'baselines') and 'revenue' in self.baselines:
            baseline = self.baselines['revenue']
            z_score = (revenue - baseline['mean']) / baseline['std'] if baseline['std'] > 0 else 0
            result.historical_avg = baseline['mean']
            result.historical_std = baseline['std']
            result.z_score = z_score
            
            if abs(z_score) > self.THRESHOLDS['revenue_std_threshold']:
                result.is_anomaly = True
                direction = "above" if z_score > 0 else "below"
                self._add_alert(
                    name="Revenue Anomaly",
                    severity=AlertSeverity.WARNING,
                    message=f"Daily revenue (${revenue:,.0f}) is {abs(z_score):.1f} std {direction} average (${baseline['mean']:,.0f})",
                    metric_name='daily_revenue',
                    current_value=revenue,
                    threshold=baseline['mean']
                )
        
        self.metrics['revenue'] = result
    
    def _check_conversions(self, conversions: pd.DataFrame) -> None:
        """Check daily conversion count."""
        conv_count = len(conversions)
        
        result = MetricResult(
            metric_name='daily_conversions',
            current_value=conv_count
        )
        
        if conv_count < self.THRESHOLDS['min_daily_conversions']:
            self._add_alert(
                name="Low Conversions",
                severity=AlertSeverity.CRITICAL,
                message=f"Daily conversions ({conv_count}) below minimum ({self.THRESHOLDS['min_daily_conversions']})",
                metric_name='daily_conversions',
                current_value=conv_count,
                threshold=self.THRESHOLDS['min_daily_conversions']
            )
        
        self.metrics['conversions'] = result
    
    def _check_average_order_value(self, conversions: pd.DataFrame) -> None:
        """Check AOV for anomalies."""
        if len(conversions) == 0:
            return
        
        aov = conversions['revenue'].mean()
        
        result = MetricResult(
            metric_name='average_order_value',
            current_value=aov
        )
        
        # Statistical check
        if hasattr(self, 'baselines') and 'aov' in self.baselines:
            baseline = self.baselines['aov']
            z_score = (aov - baseline['mean']) / baseline['std'] if baseline['std'] > 0 else 0
            result.z_score = z_score
            
            if abs(z_score) > self.THRESHOLDS['aov_std_threshold']:
                result.is_anomaly = True
                self._add_alert(
                    name="AOV Anomaly",
                    severity=AlertSeverity.WARNING,
                    message=f"AOV (${aov:,.0f}) deviates significantly from average (${baseline['mean']:,.0f})",
                    metric_name='aov',
                    current_value=aov,
                    threshold=baseline['mean']
                )
        
        self.metrics['aov'] = result
    
    # =========================================================================
    # DATA INTEGRITY CHECKS
    # =========================================================================
    
    def _check_duplicate_transactions(self, conversions: pd.DataFrame) -> None:
        """Check for duplicate transaction IDs."""
        if 'transaction_id' not in conversions.columns:
            return
        
        duplicates = conversions['transaction_id'].duplicated().sum()
        
        self.metrics['duplicate_transactions'] = MetricResult(
            metric_name='duplicate_transactions',
            current_value=duplicates
        )
        
        if duplicates > self.THRESHOLDS['max_duplicate_transactions']:
            self._add_alert(
                name="Duplicate Transactions",
                severity=AlertSeverity.CRITICAL,
                message=f"Found {duplicates} duplicate transaction IDs",
                metric_name='duplicate_transactions',
                current_value=duplicates,
                threshold=self.THRESHOLDS['max_duplicate_transactions']
            )
    
    def _check_conversion_funnel(self, sessions: pd.DataFrame) -> None:
        """Check for funnel anomalies."""
        if len(sessions) == 0:
            return
        
        atc_rate = sessions['has_add_to_cart'].mean()
        checkout_rate = sessions['has_checkout_started'].mean()
        purchase_rate = sessions['has_purchase'].mean()
        
        # Check for inverted funnel (more purchases than checkouts)
        if purchase_rate > checkout_rate:
            self._add_alert(
                name="Funnel Anomaly",
                severity=AlertSeverity.WARNING,
                message=f"Purchase rate ({purchase_rate:.2%}) exceeds checkout rate ({checkout_rate:.2%})",
                metric_name='funnel_integrity',
                current_value=purchase_rate,
                threshold=checkout_rate
            )
        
        # Check for zero conversions despite cart activity
        if atc_rate > 0 and purchase_rate == 0:
            self._add_alert(
                name="Zero Conversions",
                severity=AlertSeverity.CRITICAL,
                message=f"No purchases despite {atc_rate:.1%} add-to-cart rate",
                metric_name='purchase_rate',
                current_value=0,
                threshold=0.001
            )
    
    # =========================================================================
    # TRAFFIC HEALTH CHECKS
    # =========================================================================
    
    def _check_traffic_sources(self, sessions: pd.DataFrame) -> None:
        """Check traffic source distribution for anomalies."""
        if 'marketing_channel' not in sessions.columns:
            return
        
        channel_dist = sessions['marketing_channel'].value_counts(normalize=True)
        
        # Check if any single channel dominates unexpectedly (>80%)
        max_channel = channel_dist.index[0]
        max_share = channel_dist.iloc[0]
        
        if max_share > 0.8:
            self._add_alert(
                name="Traffic Concentration",
                severity=AlertSeverity.WARNING,
                message=f"Channel '{max_channel}' represents {max_share:.1%} of traffic",
                metric_name='channel_concentration',
                current_value=max_share,
                threshold=0.8
            )
        
        # Check for missing expected channels
        expected_channels = {'Direct', 'Paid Search', 'Organic Search'}
        missing = expected_channels - set(channel_dist.index)
        
        if missing:
            self._add_alert(
                name="Missing Traffic Channel",
                severity=AlertSeverity.WARNING,
                message=f"No traffic from expected channels: {missing}",
                metric_name='channel_coverage',
                current_value=len(channel_dist),
                threshold=len(expected_channels)
            )
    
    # =========================================================================
    # REPORTING
    # =========================================================================
    
    def generate_report(self) -> str:
        """Generate monitoring report."""
        lines = [
            "="*60,
            "DAILY MONITORING REPORT",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "="*60,
            ""
        ]
        
        # Alert summary
        critical_alerts = [a for a in self.alerts if a.severity == AlertSeverity.CRITICAL]
        warning_alerts = [a for a in self.alerts if a.severity == AlertSeverity.WARNING]
        
        if critical_alerts:
            lines.append("🚨 CRITICAL ALERTS:")
            for alert in critical_alerts:
                lines.append(f"   • {alert.message}")
            lines.append("")
        
        if warning_alerts:
            lines.append("⚠️ WARNINGS:")
            for alert in warning_alerts:
                lines.append(f"   • {alert.message}")
            lines.append("")
        
        if not self.alerts:
            lines.append("✅ All checks passed")
            lines.append("")
        
        # Metric summary
        lines.append("-"*60)
        lines.append("METRICS SUMMARY:")
        lines.append("-"*60)
        
        for name, metric in self.metrics.items():
            anomaly_flag = " ⚠️" if metric.is_anomaly else ""
            if metric.z_score is not None:
                lines.append(f"{name}: {metric.current_value:,.2f} (z={metric.z_score:.2f}){anomaly_flag}")
            else:
                lines.append(f"{name}: {metric.current_value:,.2f}")
        
        return "\n".join(lines)


def run_daily_monitoring(data_dir: str, output_dir: str) -> List[Alert]:
    """
    Run daily monitoring on the most recent data.
    
    In production, this would:
    1. Load today's data from the warehouse
    2. Load historical data for baseline comparison
    3. Run all checks
    4. Send alerts to Slack/PagerDuty/email
    5. Log metrics to monitoring dashboard
    """
    logger.info("Starting daily monitoring run...")
    
    # For demo: load all data and simulate daily monitoring
    files = sorted([f for f in os.listdir(data_dir) if f.startswith('events_') and f.endswith('.csv')])
    
    # Build historical baseline from first 7 days
    historical_metrics = []
    for f in files[:7]:
        df = pd.read_csv(os.path.join(data_dir, f), dtype=str)
        if 'clientId' in df.columns:
            df = df.rename(columns={'clientId': 'client_id'})
        
        purchases = df[df['event_name'] == 'checkout_completed']
        revenue = 0
        for _, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                revenue += float(data.get('revenue', 0))
            except:
                pass
        
        historical_metrics.append({
            'date': f.replace('events_', '').replace('.csv', ''),
            'events': len(df),
            'conversions': len(purchases),
            'revenue': revenue,
            'aov': revenue / len(purchases) if len(purchases) > 0 else 0
        })
    
    historical_df = pd.DataFrame(historical_metrics)
    
    # Initialize monitor with historical data
    monitor = DataMonitor(historical_data=historical_df)
    
    # Run checks on each day (simulating daily runs)
    all_alerts = []
    for f in files:
        date = f.replace('events_', '').replace('.csv', '')
        logger.info(f"\nChecking {date}...")
        
        # Load today's data
        df = pd.read_csv(os.path.join(data_dir, f), dtype=str)
        if 'clientId' in df.columns:
            df = df.rename(columns={'clientId': 'client_id'})
        if 'referrer' not in df.columns:
            df['referrer'] = ''
        
        # Create simple session and conversion data
        sessions = pd.DataFrame({
            'session_id': range(len(df) // 10),  # Simplified
            'client_id': df['client_id'].iloc[:len(df)//10] if len(df) > 10 else df['client_id'],
            'has_add_to_cart': [False] * (len(df) // 10),
            'has_checkout_started': [False] * (len(df) // 10),
            'has_purchase': [False] * (len(df) // 10),
            'marketing_channel': ['Direct'] * (len(df) // 10)
        })
        
        # Build conversions data
        purchases = df[df['event_name'] == 'checkout_completed'].copy()
        conversions = []
        for _, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                conversions.append({
                    'transaction_id': data.get('transaction_id'),
                    'revenue': float(data.get('revenue', 0))
                })
            except:
                pass
        conversions_df = pd.DataFrame(conversions) if conversions else pd.DataFrame(columns=['transaction_id', 'revenue'])
        
        # Run monitoring
        alerts = monitor.check_all(df, sessions, conversions_df)
        
        if alerts:
            print(f"\n{date}: {len(alerts)} alert(s)")
            for alert in alerts:
                print(f"  [{alert.severity.value}] {alert.name}: {alert.message}")
            all_alerts.extend(alerts)
    
    # Generate final report
    print("\n" + monitor.generate_report())
    
    return all_alerts


if __name__ == "__main__":
    import sys
    
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "./monitoring"
    
    os.makedirs(output_dir, exist_ok=True)
    alerts = run_daily_monitoring(data_dir, output_dir)
    
    # Exit with error code if critical alerts
    critical = [a for a in alerts if a.severity == AlertSeverity.CRITICAL]
    if critical:
        logger.error(f"Exiting with {len(critical)} critical alerts")
        exit(1)
