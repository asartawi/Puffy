# Part 1: Data Quality Framework Documentation

## Overview

This document describes the incoming data quality validation framework built to detect and prevent data quality issues before they impact production analytics. The framework was developed after analyzing 14 days of event data (Feb 23 - Mar 8, 2025) where revenue discrepancies were reported mid-period.

## Issues Identified in the Dataset

Our framework identified **five critical data quality issues** that occurred during this period:

### 1. **Missing Referrer Column (CRITICAL)** - Starting March 4, 2025
- **Impact**: The `referrer` column completely disappeared from the schema starting March 4th
- **Files affected**: `events_20250304.csv` through `events_20250308.csv` (5 days)
- **Business impact**: **Attribution is completely broken** for these days. Without referrer data, we cannot determine traffic sources, making marketing spend optimization impossible.

### 2. **NULL Client ID Spike (CRITICAL)** - Starting March 2, 2025
- **Pattern**: NULL `client_id` rate jumped from ~1% to 15.8% on March 2nd
- **Sustained issue**: Remained elevated (7.9% - 14.3%) through March 8th
- **Business impact**: ~10% of events cannot be tied to user sessions. This affects:
  - Sessionization accuracy
  - User journey analysis
  - Attribution accuracy
  - Funnel analysis

| Date | NULL Rate | Status |
|------|-----------|--------|
| Feb 23-Mar 1 | 0.5% - 1.5% | Normal |
| Mar 2 | **15.8%** | Anomaly spike |
| Mar 3-8 | 7.9% - 14.3% | Elevated |

### 3. **Duplicate Transaction IDs (CRITICAL)** - Multiple Days
- **Occurrences found**:
  - `ORD-20250226-400`: 2 occurrences (different clients, different amounts: $3,199 vs $1,524)
  - `ORD-20250227-262`: 2 occurrences
  - `ORD-20250301-176`: 2 occurrences
  - `ORD-20250308-149`: 2 occurrences (exact duplicate - same amount $1,599)
- **Business impact**: Revenue is being **double-counted** for some transactions, directly explaining the "revenue numbers looked incorrect" issue reported.

### 4. **Column Naming Convention Change (HIGH)** - Starting Feb 27, 2025
- **Issue**: Column `client_id` changed to `clientId` (camelCase)
- **Files affected**: `events_20250227.csv` onwards
- **Business impact**: Pipeline breaks if not handled; requires schema normalization in ETL

### 5. **NULL Item Prices in Purchase Events (MEDIUM)** - Starting Mar 6, 2025
- **Pattern**: 100% of item prices became NULL starting March 6th
- **Impact**: Cannot calculate average order value at item level, affects merchandising analytics

## What the Framework Checks

The framework implements 13 validation checks across 4 categories:

### Schema Validations
| Check | Severity | Purpose |
|-------|----------|---------|
| Required Columns Present | CRITICAL | Ensures all expected columns exist |
| Column Naming Convention | HIGH | Detects schema drift (e.g., `client_id` vs `clientId`) |
| Referrer Column Present | CRITICAL | Explicit check for attribution-critical column |

### Data Completeness
| Check | Severity | Purpose |
|-------|----------|---------|
| Client ID Completeness | CRITICAL | Flags when NULL rate exceeds 5% threshold |

### Data Integrity
| Check | Severity | Purpose |
|-------|----------|---------|
| Timestamp Format | HIGH | Validates ISO 8601 format |
| Timestamp Date Match | HIGH | Ensures events belong to their file date partition |
| Event Name Values | CRITICAL | Validates against allowed event types |
| Page URL Domain | MEDIUM | Ensures URLs are from valid Puffy domains |
| Event Data JSON | HIGH | Validates JSON structure in event_data |
| Duplicate Transactions | CRITICAL | Detects duplicate transaction IDs |

### Business Logic
| Check | Severity | Purpose |
|-------|----------|---------|
| Purchase Events Validation | HIGH | Validates revenue values, transaction IDs, item prices |
| Event Volume | HIGH | Detects abnormal daily event counts |
| Funnel Integrity | MEDIUM | Checks for orphan conversion events |

### Cross-File Analysis
- **Duplicate transactions across files**: Catches transaction IDs appearing in multiple daily files
- **Trend anomaly detection**: Detects sudden spikes in NULL rates, revenue drops, schema changes

## How to Run

```bash
# Run on a directory of event files
python data_quality_framework.py /path/to/data/

# Or import as a module
from data_quality_framework import DataQualityValidator

validator = DataQualityValidator()
report = validator.validate_file('events_20250301.csv')
print(report.summary())

# For batch validation with cross-file analysis
reports, cross_file = validator.validate_batch(file_paths)
```

## Future-Proofing: What Else It Would Catch

The framework is designed to catch not just these specific issues, but similar patterns:

1. **Any column disappearing from schema** (not just referrer)
2. **Any significant spike in NULL values** for critical fields
3. **Transaction duplicates** across any time period
4. **Schema drift** with any column naming changes
5. **Invalid event types** if new malformed events appear
6. **Volume anomalies** if event collection drops or spikes
7. **Revenue anomalies** including $0, negative, or missing values
8. **JSON parsing failures** in event_data

## Recommendations

Based on the issues found, we recommend:

1. **Immediate**: Investigate the tracking code change deployed around March 2-4 that caused:
   - Referrer column to stop being captured
   - Client ID tracking to degrade

2. **Pipeline**: Add this validation framework as a gate before data enters the warehouse. Files failing CRITICAL checks should be quarantined for review.

3. **Alerting**: Set up alerts for:
   - Schema changes (new/missing columns)
   - NULL rate spikes > 2x historical average
   - Duplicate transaction IDs
   - Revenue outliers

4. **Data Recovery**: For the affected period (Mar 4-8), referrer data may be recoverable from:
   - Web server access logs
   - CDN logs
   - Alternative tracking systems (Google Analytics, etc.)
