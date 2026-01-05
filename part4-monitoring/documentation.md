# Part 4: Production Monitoring Documentation

## Overview

This document describes the production monitoring system designed to ensure data accuracy for Puffy's analytics pipeline. The system monitors daily data quality, business metrics, and pipeline health, generating alerts when anomalies are detected.

## Monitoring Philosophy

The monitoring system is designed around three principles:

1. **Catch issues early** - Detect problems before they impact business decisions
2. **Minimize alert fatigue** - Only alert on actionable issues
3. **Provide context** - Include enough information to diagnose and fix issues

## What We Monitor

### 1. Data Freshness & Completeness

| Metric | Threshold | Severity | Why It Matters |
|--------|-----------|----------|----------------|
| Daily event count | < 2,000 | CRITICAL | Indicates tracking failure or site outage |
| Daily event count | > 8,000 | WARNING | Possible bot traffic or duplicate events |
| NULL client_id rate | > 5% | CRITICAL | Breaks sessionization and attribution |
| Missing columns | Any | CRITICAL | Schema change will break pipelines |

**Detection Method**: Simple threshold comparison against expected ranges.

### 2. Revenue & Conversion Metrics

| Metric | Threshold | Severity | Why It Matters |
|--------|-----------|----------|----------------|
| Daily revenue | < $10,000 | CRITICAL | Significant business issue or tracking failure |
| Daily conversions | < 10 | CRITICAL | Checkout tracking may be broken |
| Revenue anomaly | > 2.5 std | WARNING | Unusual day - investigate cause |
| AOV anomaly | > 2.0 std | WARNING | Pricing issue or data problem |

**Detection Method**: 
- Absolute thresholds for minimum acceptable values
- Statistical anomaly detection using z-scores against 7-day rolling average

### 3. Data Integrity

| Metric | Threshold | Severity | Why It Matters |
|--------|-----------|----------|----------------|
| Duplicate transactions | > 0 | CRITICAL | Revenue being double-counted |
| Funnel inversion | purchases > checkouts | WARNING | Tracking gap in funnel |
| Zero conversions with cart activity | Any | CRITICAL | Checkout tracking broken |

**Detection Method**: Direct counting and logical consistency checks.

### 4. Traffic Source Health

| Metric | Threshold | Severity | Why It Matters |
|--------|-----------|----------|----------------|
| Single channel > 80% | Any | WARNING | Unusual concentration, possible tracking issue |
| Missing expected channels | Any | WARNING | Attribution tracking may be broken |

**Detection Method**: Distribution analysis across channels.

## Alert Severity Levels

### CRITICAL
- **Response time**: Immediate (within 1 hour)
- **Escalation**: Auto-page on-call engineer
- **Examples**: 
  - NULL client_id spike > 5%
  - Duplicate transactions detected
  - Schema change (missing columns)
  - Revenue below $10,000

### WARNING
- **Response time**: Same business day
- **Escalation**: Slack notification to #data-alerts channel
- **Examples**:
  - Revenue anomaly (statistical)
  - Traffic concentration
  - AOV deviation

### INFO
- **Response time**: Weekly review
- **Escalation**: Dashboard metric, no notification
- **Examples**:
  - Minor funnel variations
  - Expected seasonal patterns

## Threshold Tuning

Thresholds are configured in `monitoring_system.py`:

```python
THRESHOLDS = {
    # Absolute thresholds
    'min_daily_events': 2000,
    'max_daily_events': 8000,
    'min_daily_conversions': 10,
    'min_daily_revenue': 10000,
    'max_null_client_rate': 0.05,
    'max_duplicate_transactions': 0,
    
    # Statistical thresholds (standard deviations)
    'revenue_std_threshold': 2.5,
    'conversion_std_threshold': 2.5,
    'traffic_std_threshold': 3.0,
    'aov_std_threshold': 2.0,
}
```

**Tuning recommendations**:
- Start with conservative thresholds (fewer alerts)
- Review false positive rate weekly
- Adjust based on seasonality (weekends, holidays)
- Document threshold changes with rationale

## Implementation Details

### Daily Run Schedule

```
┌─────────────────────────────────────────────────────────────┐
│ 6:00 AM UTC - Data arrives in warehouse                     │
│ 6:30 AM UTC - Monitoring job starts                         │
│ 6:35 AM UTC - Alerts sent (if any)                          │
│ 7:00 AM UTC - Daily report generated                        │
└─────────────────────────────────────────────────────────────┘
```

### Historical Baseline Calculation

The system uses a **7-day rolling average** for statistical anomaly detection:

```python
# Baseline is recalculated daily from the previous 7 days
baseline_mean = historical_data[-7:]['revenue'].mean()
baseline_std = historical_data[-7:]['revenue'].std()

# Z-score calculation
z_score = (today_value - baseline_mean) / baseline_std

# Alert if |z| > threshold
if abs(z_score) > 2.5:
    trigger_alert()
```

**Why 7 days?**
- Captures day-of-week patterns
- Short enough to adapt to trends
- Long enough for stable statistics

### Integration Points

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Airflow   │────▶│  Monitoring │────▶│    Slack    │
│  Scheduler  │     │   System    │     │   Alerts    │
└─────────────┘     └──────┬──────┘     └─────────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │  PagerDuty  │
                   │ (Critical)  │
                   └─────────────┘
```

## Running the Monitor

### Command Line

```bash
# Run monitoring on a data directory
python monitoring_system.py /path/to/data /path/to/output

# In production (as part of Airflow DAG)
python monitoring_system.py \
  --data-source bigquery://project.dataset.events \
  --alert-channel slack://data-alerts \
  --pagerduty-key $PAGERDUTY_KEY
```

### Output

The system generates:
1. **Console output**: Summary of alerts and metrics
2. **JSON report**: Machine-readable for dashboards
3. **Slack notifications**: For WARNING+ severity
4. **PagerDuty alerts**: For CRITICAL severity

## Alert Response Playbooks

### High NULL Client ID Rate

**Symptoms**: NULL client_id rate > 5%

**Investigation steps**:
1. Check tracking code deployment history
2. Review browser console for JS errors
3. Check ad blocker/privacy tool adoption
4. Verify cookie consent implementation

**Resolution**: Typically requires frontend engineering fix

### Duplicate Transactions

**Symptoms**: Same transaction_id appears multiple times

**Investigation steps**:
1. Check checkout page for double-submit protection
2. Review webhook retry logic
3. Check for payment processor duplicates

**Resolution**: Add deduplication at checkout event capture

### Schema Change

**Symptoms**: Missing expected columns

**Investigation steps**:
1. Check tracking code version
2. Review recent deployments
3. Contact frontend team

**Resolution**: Revert tracking code or update pipeline to handle new schema

## Practical Considerations

### Avoiding Alert Fatigue

1. **Consolidate related alerts**: One "Data Quality Issues" alert vs. many small ones
2. **Implement snooze**: Allow temporary suppression for known issues
3. **Weekly threshold review**: Adjust based on false positive rate
4. **Severity tiers**: Only page for truly critical issues

### Handling Expected Variations

- **Weekends**: Lower thresholds for conversions (expected)
- **Holidays**: Pre-scheduled threshold adjustments
- **Promotions**: Temporary upper bound increases

### Monitoring the Monitor

Track:
- Alert volume per day
- False positive rate
- Mean time to resolution
- Coverage gaps (issues found by humans first)

## Sample Alert Output

```
============================================================
DAILY MONITORING REPORT
Generated: 2025-03-04 06:35:00 UTC
============================================================

🚨 CRITICAL ALERTS:
   • NULL client_id rate (8.5%) exceeds threshold (5.0%)
   • Missing columns: {'referrer'}

⚠️ WARNINGS:
   • Daily revenue ($12,415) is 2.1 std below average ($20,541)

------------------------------------------------------------
METRICS SUMMARY:
------------------------------------------------------------
event_count: 3,538.00
null_client_rate: 0.09
revenue: 12,415.00 (z=-2.14) ⚠️
conversions: 20.00
aov: 620.75
duplicate_transactions: 0.00
```

## Next Steps for Production

1. **Integrate with Airflow**: Schedule as DAG task after transformation
2. **Connect to Slack**: Use Slack webhook for alerts
3. **Set up PagerDuty**: For critical alert escalation
4. **Build dashboard**: Grafana or Looker for metric visualization
5. **Implement incident tracking**: Link alerts to JIRA tickets
