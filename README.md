# Puffy Analytics Data Infrastructure

A complete data infrastructure solution for Puffy's marketing analytics, including data quality validation, transformation pipelines, attribution modeling, and production monitoring.

## Project Structure

```
puffy-analytics/
├── part1-data-quality/
│   ├── data_quality_framework.py    # Validation framework
│   └── documentation.md             # Data quality documentation
│
├── part2-transformation/
│   ├── transformation_pipeline.py   # ETL pipeline
│   ├── validation_tests.py          # Transformation tests
│   ├── documentation.md             # Pipeline documentation
│   └── output/                      # Transformed tables
│       ├── events_enriched.csv
│       ├── sessions.csv
│       ├── conversions.csv
│       ├── attribution.csv
│       ├── channel_summary.csv
│       └── user_journeys.csv
│
├── part3-analysis/
│   ├── executive-summary.md         # Business analysis
│   └── supporting-analysis/         # Charts and visualizations
│
├── part4-monitoring/
│   ├── monitoring_system.py         # Production monitoring
│   └── documentation.md             # Monitoring documentation
│
└── README.md                        # This file
```

## Quick Start

### Prerequisites

- Python 3.8+
- pandas
- matplotlib (for visualizations)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/puffy-analytics.git
cd puffy-analytics

# Install dependencies
pip install pandas matplotlib
```

### Running the Pipeline

```bash
# 1. Run data quality validation
python part1-data-quality/data_quality_framework.py /path/to/raw/data

# 2. Run transformation pipeline
python part2-transformation/transformation_pipeline.py /path/to/raw/data ./output

# 3. Run validation tests
python part2-transformation/validation_tests.py /path/to/raw/data ./output

# 4. Run production monitoring
python part4-monitoring/monitoring_system.py /path/to/raw/data ./monitoring
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RAW EVENT DATA                                   │
│                    (14 daily CSV files)                                  │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    PART 1: DATA QUALITY                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │ Schema Checks   │  │ Completeness    │  │ Integrity       │          │
│  │ • Column names  │  │ • NULL rates    │  │ • Duplicates    │          │
│  │ • Data types    │  │ • Missing data  │  │ • Valid values  │          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ Validated Data
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    PART 2: TRANSFORMATION                                │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ 1. Load & Clean → 2. Extract Traffic → 3. Sessionize            │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                │                                         │
│                                ▼                                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐       │
│  │  Sessions   │ │ Conversions │ │ Attribution │ │  Channel    │       │
│  │   Table     │ │   Table     │ │   Table     │ │  Summary    │       │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    PART 3: BUSINESS ANALYSIS                             │
│  • Channel Performance    • Conversion Funnel    • User Journeys        │
│  • Attribution Comparison • Device Analysis      • Daily Trends         │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    PART 4: PRODUCTION MONITORING                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │ Data Quality    │  │ Business        │  │ Pipeline        │          │
│  │ Alerts          │  │ Metric Alerts   │  │ Health Alerts   │          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Findings Summary

### Data Quality Issues Identified
1. **Missing referrer column** (March 4-8) - Breaks attribution
2. **NULL client_id spike** (15.8% on March 2) - Affects sessionization
3. **Duplicate transactions** (4 found) - Inflated revenue by ~$6,700
4. **Column naming change** (client_id → clientId on Feb 27)

### Business Insights
- **$287,744** total revenue over 14 days
- **$992** average order value (strong premium positioning)
- **55%** of revenue from Direct traffic
- **Paid Search** shows 22% attribution shift (first-click to last-click)
- **94.6%** of visitors leave without adding to cart (optimization opportunity)

## Output Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `sessions.csv` | Session-level aggregation | session_id, client_id, traffic_source, conversions |
| `conversions.csv` | Transaction records | transaction_id, revenue, items |
| `attribution.csv` | First & last click attribution | first_click_channel, last_click_channel |
| `channel_summary.csv` | Daily channel performance | date, channel, conversions, revenue |
| `user_journeys.csv` | Converting user paths | sessions_to_convert, channel_path |

## Configuration

Key parameters can be adjusted in the respective Python files:

### Sessionization
```python
SESSION_TIMEOUT_MINUTES = 30  # Inactivity timeout
```

### Attribution
```python
ATTRIBUTION_LOOKBACK_DAYS = 7  # Lookback window
```

### Monitoring Thresholds
```python
THRESHOLDS = {
    'max_null_client_rate': 0.05,
    'min_daily_revenue': 10000,
    'revenue_std_threshold': 2.5,
}
```

## Contributing

1. Run all tests before submitting changes
2. Update documentation for any new features
3. Follow existing code style

## License

Proprietary - Puffy Internal Use Only
