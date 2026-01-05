# Part 2: Transformation Pipeline Documentation

## Overview

This document describes the transformation layer that converts raw event data into analytics-ready tables. The pipeline produces six output tables that power marketing attribution, user behavior analysis, and conversion tracking.

## Architecture

```
┌─────────────────┐
│  Raw Event CSVs │
│   (14 files)    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│         LOAD & CLEAN                     │
│  • Normalize column names               │
│  • Handle missing referrer column       │
│  • Remove NULL client_ids               │
│  • Parse timestamps                     │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│      TRAFFIC SOURCE EXTRACTION          │
│  • Parse UTM parameters                 │
│  • Identify gclid (Google Ads)          │
│  • Analyze referrer domains             │
│  • Classify marketing channels          │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│          SESSIONIZATION                  │
│  • 30-minute inactivity timeout         │
│  • Generate unique session_ids          │
│  • Track session boundaries             │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│         OUTPUT TABLES                    │
├─────────────────────────────────────────┤
│  • events_enriched                      │
│  • sessions                             │
│  • conversions                          │
│  • attribution                          │
│  • channel_summary                      │
│  • user_journeys                        │
└─────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Session Definition

**Approach**: 30-minute inactivity timeout (industry standard)

A new session starts when:
- First event for a client, OR
- More than 30 minutes since the previous event

**Why this approach**:
- Matches Google Analytics methodology for comparability
- Captures distinct browsing "intents" - if a user leaves and comes back later, they're likely in a different mindset
- Industry-validated timeout that balances session fragmentation vs. session merging

**Trade-off**: Some multi-tab browsing or users who step away briefly may be split into multiple sessions. However, this is preferable to merging unrelated browsing intents.

### 2. Traffic Source Extraction

**Priority order for source attribution**:
1. **gclid parameter** → Google Ads (highest confidence - actual click ID)
2. **UTM parameters** → Use source/medium/campaign as provided
3. **Referrer domain** → Classify based on known patterns
4. **Default** → Direct

**Why this priority**:
- gclid is a definitive indicator of a paid Google click
- UTM parameters are explicitly set by marketing team
- Referrer is helpful but can be blocked/missing
- Direct is the fallback when no attribution data exists

### 3. Marketing Channel Classification

| Channel | Detection Logic |
|---------|-----------------|
| Paid Search | gclid present OR medium = 'cpc' |
| Paid Social | source = facebook/instagram + medium = cpc |
| Email | source = email/klaviyo OR medium = email |
| Organic Search | source = google/bing + medium = organic |
| Affiliate | Anonymized source domains (source-*.com) |
| Referral | medium = referral |
| Direct | No attribution data available |

### 4. Attribution Models

The pipeline supports both **first-click** and **last-click** attribution with a **7-day lookback window** (as specified).

**First-Click Attribution**:
- Credit goes to the channel that first brought the user
- Best for understanding awareness/discovery channels
- Answers: "What channels introduce new customers?"

**Last-Click Attribution**:
- Credit goes to the channel of the converting session
- Best for understanding closing channels
- Answers: "What channels drive final conversion?"

**Lookback Window (7 days)**:
- Balances recency with capturing the full journey
- Appropriate for mattress purchase cycle (high-consideration purchase)
- Excludes touchpoints older than 7 days from conversion

### 5. Handling NULL Client IDs

**Decision**: Remove events with NULL client_id (2,908 events / 5.8%)

**Rationale**:
- Cannot sessionize events without client identifier
- Cannot attribute conversions without client tracking
- These events are already compromised for analytics purposes

**Trade-off**: We lose some data, but the alternative (making up IDs) would corrupt our analysis. The removed events are primarily pageviews, not conversions.

## Output Tables

### 1. `events_enriched.csv`
Raw events with added fields for traffic source and session.

| Column | Description |
|--------|-------------|
| session_id | Unique session identifier |
| traffic_source | Extracted source (google, facebook, etc.) |
| traffic_medium | Extracted medium (cpc, organic, etc.) |
| traffic_campaign | Campaign name if available |
| marketing_channel | Classified channel |

**Row count**: 47,055

### 2. `sessions.csv`
Aggregated session-level data.

| Column | Description |
|--------|-------------|
| session_id | Unique session identifier |
| client_id | Cookie-based device identifier |
| session_start/end | Session timestamps |
| event_count | Events in session |
| session_duration_seconds | Time from first to last event |
| landing_page | First page URL |
| traffic_source/medium/campaign | Attribution data |
| marketing_channel | Classified channel |
| device_type | Desktop/Mobile/Tablet |
| has_add_to_cart | Boolean conversion flag |
| has_purchase | Boolean conversion flag |

**Row count**: 38,267

### 3. `conversions.csv`
Transaction-level data for all purchases.

| Column | Description |
|--------|-------------|
| transaction_id | Unique order identifier |
| client_id | Client who purchased |
| session_id | Converting session |
| conversion_timestamp | Time of purchase |
| revenue | Order value |
| item_count | Number of items |

**Row count**: 290 (after deduplication)

### 4. `attribution.csv`
Attribution data for each conversion.

| Column | Description |
|--------|-------------|
| transaction_id | Links to conversions table |
| first_click_* | First-touch attribution |
| last_click_* | Last-touch attribution |
| touchpoint_count | Sessions in journey |
| days_to_convert | Time from first touch |

**Row count**: 290

### 5. `channel_summary.csv`
Daily aggregated performance by channel.

| Column | Description |
|--------|-------------|
| date | Conversion date |
| channel | Marketing channel |
| conversions | Number of purchases |
| revenue | Total revenue |
| attribution_model | first_click or last_click |

**Row count**: 106 (53 first_click + 53 last_click)

### 6. `user_journeys.csv`
Aggregated view of converting users' paths.

| Column | Description |
|--------|-------------|
| client_id | Converting user |
| total_sessions | All-time session count |
| days_from_first_visit | First visit to purchase |
| channels_touched | Unique channels in journey |
| channel_path | Sequence of channels |

**Row count**: 281

## Validation Results

The transformation was validated against source data:

| Check | Original | Transformed | Notes |
|-------|----------|-------------|-------|
| Events | 49,963 | 47,055 | 2,908 NULL client_ids removed |
| Sessionization | 47,055 | 47,055 | 100% events assigned sessions |
| Conversions | 294 | 290 | 4 duplicates removed |
| Revenue | $294,461 | $287,744 | $6,717 duplicate revenue removed |
| Attribution | 290 | 290 | 100% coverage |

## Running the Pipeline

```bash
# Run transformation
python transformation_pipeline.py /path/to/raw/data /path/to/output

# Or import as module
from transformation_pipeline import run_transformation
tables = run_transformation('./data', './output')
```

## Scalability Considerations

For production deployment at scale:

1. **Incremental Processing**: Modify to process only new files, merge with existing sessions
2. **Partitioning**: Output tables should be partitioned by date
3. **Database Integration**: Replace CSV output with database writes (BigQuery, Snowflake, etc.)
4. **Parallelization**: Traffic source extraction can be parallelized
5. **Lookback Join Optimization**: Pre-compute session windows for faster attribution

## Known Limitations

1. **Cross-device tracking**: Different devices = different client_ids. Consider identity resolution if user accounts are available.

2. **Missing referrer data**: March 4-8 has no referrer column. Attribution defaults to URL parameters or direct.

3. **Anonymized sources**: Partner/affiliate sources are anonymized (source-*.com), limiting granular partner analysis.

4. **Single conversion per client**: User journey table takes first conversion only. Could be extended for repeat purchase analysis.
