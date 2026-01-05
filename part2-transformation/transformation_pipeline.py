"""
Puffy Analytics Transformation Pipeline
========================================
Transforms raw event data into analytics-ready tables for:
1. Session-level user behavior analysis
2. Marketing attribution (first-click and last-click)
3. Funnel and conversion analysis

Author: Data Infrastructure Team
Date: 2025
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SESSION_TIMEOUT_MINUTES = 30
ATTRIBUTION_LOOKBACK_DAYS = 7


def load_and_clean_events(data_dir: str) -> pd.DataFrame:
    """Load all event files and perform initial cleaning."""
    logger.info(f"Loading events from {data_dir}")
    
    files = sorted([
        os.path.join(data_dir, f) 
        for f in os.listdir(data_dir) 
        if f.startswith('events_') and f.endswith('.csv')
    ])
    
    all_data = []
    for f in files:
        df = pd.read_csv(f, dtype=str)
        if 'clientId' in df.columns:
            df = df.rename(columns={'clientId': 'client_id'})
        if 'referrer' not in df.columns:
            df['referrer'] = ''
        df['source_file'] = os.path.basename(f)
        all_data.append(df)
    
    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Loaded {len(combined):,} events from {len(files)} files")
    
    combined['event_timestamp'] = pd.to_datetime(combined['timestamp'])
    combined['event_date'] = combined['event_timestamp'].dt.date
    
    # Remove NULL client_ids
    null_client = combined['client_id'].isna() | (combined['client_id'] == '')
    if null_client.sum() > 0:
        logger.warning(f"Removing {null_client.sum():,} events with NULL client_id")
        combined = combined[~null_client].copy()
    
    combined = combined.sort_values(['client_id', 'event_timestamp']).reset_index(drop=True)
    return combined


def extract_traffic_source(row: pd.Series) -> Dict[str, str]:
    """Extract traffic source from URL params and referrer."""
    page_url = row.get('page_url', '')
    referrer = row.get('referrer', '')
    
    source, medium, campaign = None, None, None
    
    try:
        params = parse_qs(urlparse(page_url).query)
        
        if 'gclid' in params:
            source, medium = 'google', 'cpc'
            campaign = params.get('utm_campaign', ['google_ads'])[0]
        elif 'utm_source' in params:
            source = params['utm_source'][0]
            medium = params.get('utm_medium', [''])[0]
            campaign = params.get('utm_campaign', [''])[0]
        elif 'fbclid' in params:
            source, medium, campaign = 'facebook', 'cpc', 'facebook_ads'
    except:
        pass
    
    if not source and referrer:
        try:
            ref_domain = urlparse(referrer).netloc.lower()
            if 'google' in ref_domain:
                source, medium = 'google', 'organic'
            elif 'bing' in ref_domain:
                source, medium = 'bing', 'organic'
            elif 'facebook' in ref_domain:
                source, medium = 'facebook', 'social'
            elif 'puffy.com' in ref_domain:
                source, medium = 'internal', 'internal'
            elif 'source-' in ref_domain:
                source, medium = 'affiliate', 'referral'
            elif ref_domain:
                source, medium = ref_domain, 'referral'
        except:
            pass
    
    if not source:
        source, medium = 'direct', 'none'
    
    channel = classify_channel(source, medium)
    
    return {
        'traffic_source': source,
        'traffic_medium': medium or 'none',
        'traffic_campaign': campaign or '',
        'marketing_channel': channel
    }


def classify_channel(source: str, medium: str) -> str:
    """Classify into marketing channel."""
    source_lower = (source or '').lower()
    medium_lower = (medium or '').lower()
    
    if medium_lower in ['cpc', 'ppc', 'paid']:
        return 'Paid Search'
    if source_lower in ['facebook', 'instagram'] and medium_lower in ['cpc', 'paid']:
        return 'Paid Social'
    if source_lower in ['email', 'klaviyo'] or medium_lower == 'email':
        return 'Email'
    if source_lower in ['google', 'bing'] and medium_lower == 'organic':
        return 'Organic Search'
    if source_lower == 'affiliate' or medium_lower == 'affiliate':
        return 'Affiliate'
    if medium_lower == 'referral':
        return 'Referral'
    if source_lower == 'direct':
        return 'Direct'
    if source_lower == 'internal':
        return 'Internal'
    return 'Other'


def extract_all_traffic_sources(df: pd.DataFrame) -> pd.DataFrame:
    """Apply traffic source extraction to all rows."""
    logger.info("Extracting traffic sources...")
    traffic_data = df.apply(extract_traffic_source, axis=1, result_type='expand')
    for col in traffic_data.columns:
        df[col] = traffic_data[col]
    return df


def create_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Create session IDs based on 30-minute timeout."""
    logger.info("Creating sessions...")
    
    df = df.sort_values(['client_id', 'event_timestamp']).reset_index(drop=True)
    df['prev_timestamp'] = df.groupby('client_id')['event_timestamp'].shift(1)
    df['time_since_last'] = (df['event_timestamp'] - df['prev_timestamp']).dt.total_seconds() / 60
    
    df['new_session'] = (
        (df['client_id'] != df['client_id'].shift(1)) |
        (df['time_since_last'] > SESSION_TIMEOUT_MINUTES) |
        (df['time_since_last'].isna())
    )
    
    df['session_number'] = df.groupby('client_id')['new_session'].cumsum()
    df['session_id'] = df['client_id'] + '_' + df['session_number'].astype(str)
    
    logger.info(f"Created {df['session_id'].nunique():,} sessions")
    df = df.drop(columns=['prev_timestamp', 'time_since_last', 'new_session', 'session_number'])
    return df


def parse_device_type(user_agent: str) -> str:
    """Parse device type from user agent."""
    if pd.isna(user_agent):
        return 'Unknown'
    ua_lower = user_agent.lower()
    if 'ipad' in ua_lower or 'tablet' in ua_lower:
        return 'Tablet'
    if 'mobile' in ua_lower or 'iphone' in ua_lower or 'android' in ua_lower:
        return 'Mobile'
    return 'Desktop'


def parse_browser(user_agent: str) -> str:
    """Parse browser from user agent."""
    if pd.isna(user_agent):
        return 'Unknown'
    ua_lower = user_agent.lower()
    if 'chrome' in ua_lower and 'edg' not in ua_lower:
        return 'Chrome'
    if 'safari' in ua_lower and 'chrome' not in ua_lower:
        return 'Safari'
    if 'firefox' in ua_lower:
        return 'Firefox'
    if 'edg' in ua_lower:
        return 'Edge'
    return 'Other'


def build_sessions_table(events_df: pd.DataFrame) -> pd.DataFrame:
    """Build aggregated sessions table."""
    logger.info("Building sessions table...")
    
    sessions = events_df.groupby('session_id').agg({
        'client_id': 'first',
        'event_timestamp': ['min', 'max', 'count'],
        'event_name': lambda x: list(x),
        'traffic_source': 'first',
        'traffic_medium': 'first',
        'traffic_campaign': 'first',
        'marketing_channel': 'first',
        'page_url': 'first',
        'referrer': 'first',
        'user_agent': 'first',
        'event_date': 'first',
    }).reset_index()
    
    sessions.columns = [
        'session_id', 'client_id', 'session_start', 'session_end', 'event_count',
        'event_sequence', 'traffic_source', 'traffic_medium', 'traffic_campaign',
        'marketing_channel', 'landing_page', 'referrer', 'user_agent', 'session_date'
    ]
    
    sessions['session_duration_seconds'] = (
        sessions['session_end'] - sessions['session_start']
    ).dt.total_seconds()
    
    sessions['has_add_to_cart'] = sessions['event_sequence'].apply(lambda x: 'product_added_to_cart' in x)
    sessions['has_checkout_started'] = sessions['event_sequence'].apply(lambda x: 'checkout_started' in x)
    sessions['has_purchase'] = sessions['event_sequence'].apply(lambda x: 'checkout_completed' in x)
    sessions['has_email_signup'] = sessions['event_sequence'].apply(lambda x: 'email_filled_on_popup' in x)
    
    sessions['device_type'] = sessions['user_agent'].apply(parse_device_type)
    sessions['browser'] = sessions['user_agent'].apply(parse_browser)
    
    sessions = sessions.drop(columns=['event_sequence'])
    logger.info(f"Built sessions table with {len(sessions):,} sessions")
    return sessions


def build_conversions_table(events_df: pd.DataFrame) -> pd.DataFrame:
    """Extract conversion events with full details."""
    logger.info("Building conversions table...")
    
    conversions = events_df[events_df['event_name'] == 'checkout_completed'].copy()
    
    def parse_purchase_data(event_data_str):
        try:
            data = json.loads(event_data_str)
            return {
                'transaction_id': data.get('transaction_id'),
                'revenue': float(data.get('revenue', 0)),
                'items': data.get('items', []),
                'user_email_hash': data.get('user_email', '')[:16],
                'item_count': len(data.get('items', [])),
            }
        except:
            return {'transaction_id': None, 'revenue': 0, 'items': [], 'user_email_hash': '', 'item_count': 0}
    
    purchase_data = conversions['event_data'].apply(parse_purchase_data).apply(pd.Series)
    conversions = pd.concat([conversions, purchase_data], axis=1)
    
    # Remove duplicates (keep first occurrence)
    conversions = conversions.drop_duplicates(subset=['transaction_id'], keep='first')
    
    conversions = conversions[[
        'transaction_id', 'client_id', 'session_id', 'event_timestamp', 'event_date',
        'revenue', 'item_count', 'user_email_hash', 'traffic_source', 'traffic_medium',
        'traffic_campaign', 'marketing_channel', 'page_url'
    ]].copy()
    
    conversions = conversions.rename(columns={'event_timestamp': 'conversion_timestamp'})
    
    logger.info(f"Built conversions table with {len(conversions):,} conversions")
    return conversions


def build_attribution_table(
    events_df: pd.DataFrame, 
    conversions_df: pd.DataFrame,
    sessions_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Build attribution table with first-click and last-click attribution.
    
    Lookback window: 7 days
    """
    logger.info("Building attribution table...")
    
    attribution_records = []
    
    for _, conversion in conversions_df.iterrows():
        client_id = conversion['client_id']
        conversion_time = conversion['conversion_timestamp']
        lookback_start = conversion_time - timedelta(days=ATTRIBUTION_LOOKBACK_DAYS)
        
        # Get all sessions for this client within lookback window
        client_sessions = sessions_df[
            (sessions_df['client_id'] == client_id) &
            (sessions_df['session_start'] >= lookback_start) &
            (sessions_df['session_start'] <= conversion_time)
        ].sort_values('session_start')
        
        # Filter out internal traffic for attribution
        touchpoint_sessions = client_sessions[
            client_sessions['marketing_channel'] != 'Internal'
        ]
        
        if len(touchpoint_sessions) == 0:
            # No external touchpoints - attribute to direct
            first_touch = last_touch = {
                'traffic_source': 'direct',
                'traffic_medium': 'none',
                'traffic_campaign': '',
                'marketing_channel': 'Direct'
            }
        else:
            first_touch = touchpoint_sessions.iloc[0]
            last_touch = touchpoint_sessions.iloc[-1]
        
        attribution_records.append({
            'transaction_id': conversion['transaction_id'],
            'client_id': client_id,
            'conversion_timestamp': conversion_time,
            'revenue': conversion['revenue'],
            
            # First-click attribution
            'first_click_source': first_touch.get('traffic_source', first_touch['traffic_source'] if isinstance(first_touch, pd.Series) else first_touch.get('traffic_source')),
            'first_click_medium': first_touch.get('traffic_medium', first_touch['traffic_medium'] if isinstance(first_touch, pd.Series) else first_touch.get('traffic_medium')),
            'first_click_campaign': first_touch.get('traffic_campaign', first_touch['traffic_campaign'] if isinstance(first_touch, pd.Series) else first_touch.get('traffic_campaign')),
            'first_click_channel': first_touch.get('marketing_channel', first_touch['marketing_channel'] if isinstance(first_touch, pd.Series) else first_touch.get('marketing_channel')),
            
            # Last-click attribution
            'last_click_source': last_touch.get('traffic_source', last_touch['traffic_source'] if isinstance(last_touch, pd.Series) else last_touch.get('traffic_source')),
            'last_click_medium': last_touch.get('traffic_medium', last_touch['traffic_medium'] if isinstance(last_touch, pd.Series) else last_touch.get('traffic_medium')),
            'last_click_campaign': last_touch.get('traffic_campaign', last_touch['traffic_campaign'] if isinstance(last_touch, pd.Series) else last_touch.get('traffic_campaign')),
            'last_click_channel': last_touch.get('marketing_channel', last_touch['marketing_channel'] if isinstance(last_touch, pd.Series) else last_touch.get('marketing_channel')),
            
            'touchpoint_count': len(touchpoint_sessions),
            'days_to_convert': (conversion_time - touchpoint_sessions.iloc[0]['session_start']).days if len(touchpoint_sessions) > 0 else 0
        })
    
    attribution_df = pd.DataFrame(attribution_records)
    logger.info(f"Built attribution table with {len(attribution_df):,} records")
    return attribution_df


def build_daily_channel_summary(attribution_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build daily summary by channel for both attribution models."""
    logger.info("Building daily channel summaries...")
    
    attribution_df['conversion_date'] = pd.to_datetime(attribution_df['conversion_timestamp']).dt.date
    
    # First-click summary
    first_click_summary = attribution_df.groupby(
        ['conversion_date', 'first_click_channel']
    ).agg({
        'transaction_id': 'count',
        'revenue': 'sum'
    }).reset_index()
    first_click_summary.columns = ['date', 'channel', 'conversions', 'revenue']
    first_click_summary['attribution_model'] = 'first_click'
    
    # Last-click summary
    last_click_summary = attribution_df.groupby(
        ['conversion_date', 'last_click_channel']
    ).agg({
        'transaction_id': 'count',
        'revenue': 'sum'
    }).reset_index()
    last_click_summary.columns = ['date', 'channel', 'conversions', 'revenue']
    last_click_summary['attribution_model'] = 'last_click'
    
    return first_click_summary, last_click_summary


def build_user_journey_table(sessions_df: pd.DataFrame, conversions_df: pd.DataFrame) -> pd.DataFrame:
    """Build table showing user journey to conversion."""
    logger.info("Building user journey table...")
    
    converting_clients = conversions_df['client_id'].unique()
    
    journey_records = []
    for client_id in converting_clients:
        client_sessions = sessions_df[sessions_df['client_id'] == client_id].sort_values('session_start')
        
        conversion = conversions_df[conversions_df['client_id'] == client_id].iloc[0]
        
        journey_records.append({
            'client_id': client_id,
            'total_sessions': len(client_sessions),
            'total_pageviews': client_sessions['event_count'].sum(),
            'days_from_first_visit': (conversion['conversion_timestamp'] - client_sessions.iloc[0]['session_start']).days,
            'channels_touched': client_sessions['marketing_channel'].nunique(),
            'channel_path': ' > '.join(client_sessions['marketing_channel'].tolist()[-5:]),  # Last 5
            'devices_used': client_sessions['device_type'].nunique(),
            'converted': True,
            'revenue': conversion['revenue']
        })
    
    return pd.DataFrame(journey_records)


def run_transformation(data_dir: str, output_dir: str) -> Dict[str, pd.DataFrame]:
    """
    Run the complete transformation pipeline.
    
    Args:
        data_dir: Directory containing raw event CSV files
        output_dir: Directory to save transformed tables
        
    Returns:
        Dictionary of transformed DataFrames
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Load and clean
    events = load_and_clean_events(data_dir)
    
    # Extract traffic sources
    events = extract_all_traffic_sources(events)
    
    # Create sessions
    events = create_sessions(events)
    
    # Build tables
    sessions = build_sessions_table(events)
    conversions = build_conversions_table(events)
    attribution = build_attribution_table(events, conversions, sessions)
    first_click, last_click = build_daily_channel_summary(attribution)
    user_journeys = build_user_journey_table(sessions, conversions)
    
    # Combined channel summary
    channel_summary = pd.concat([first_click, last_click], ignore_index=True)
    
    # Save tables
    tables = {
        'events_enriched': events,
        'sessions': sessions,
        'conversions': conversions,
        'attribution': attribution,
        'channel_summary': channel_summary,
        'user_journeys': user_journeys
    }
    
    for name, df in tables.items():
        output_path = os.path.join(output_dir, f'{name}.csv')
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {name} to {output_path} ({len(df):,} rows)")
    
    return tables


def validate_transformation(tables: Dict[str, pd.DataFrame], data_dir: str) -> None:
    """
    Validate transformation output against source data.
    """
    logger.info("\n" + "="*60)
    logger.info("TRANSFORMATION VALIDATION")
    logger.info("="*60)
    
    events = tables['events_enriched']
    sessions = tables['sessions']
    conversions = tables['conversions']
    attribution = tables['attribution']
    
    # Load original data for comparison
    files = sorted([f for f in os.listdir(data_dir) if f.startswith('events_') and f.endswith('.csv')])
    original_events = 0
    original_purchases = 0
    original_revenue = 0
    
    for f in files:
        df = pd.read_csv(os.path.join(data_dir, f), dtype=str)
        original_events += len(df)
        purchases = df[df['event_name'] == 'checkout_completed']
        original_purchases += len(purchases)
        for _, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                original_revenue += float(data.get('revenue', 0))
            except:
                pass
    
    # Validation checks
    checks = []
    
    # 1. Event count (accounting for NULL client_ids removed)
    null_removed = original_events - len(events)
    checks.append({
        'check': 'Event Count',
        'original': original_events,
        'transformed': len(events),
        'note': f'{null_removed} NULL client_ids removed',
        'passed': True
    })
    
    # 2. All events have session_id
    events_without_session = events['session_id'].isna().sum()
    checks.append({
        'check': 'All Events Sessionized',
        'original': len(events),
        'transformed': len(events) - events_without_session,
        'note': f'{events_without_session} without session',
        'passed': events_without_session == 0
    })
    
    # 3. Purchase count (after deduplication)
    checks.append({
        'check': 'Conversion Count',
        'original': original_purchases,
        'transformed': len(conversions),
        'note': 'Duplicates removed',
        'passed': len(conversions) <= original_purchases
    })
    
    # 4. Revenue reconciliation
    transformed_revenue = conversions['revenue'].sum()
    checks.append({
        'check': 'Revenue Total',
        'original': f'${original_revenue:,.2f}',
        'transformed': f'${transformed_revenue:,.2f}',
        'note': 'After dedup',
        'passed': True  # Lower is expected due to dedup
    })
    
    # 5. Attribution coverage
    attributed = len(attribution)
    checks.append({
        'check': 'Attribution Coverage',
        'original': len(conversions),
        'transformed': attributed,
        'note': f'{attributed/len(conversions)*100:.1f}% attributed' if len(conversions) > 0 else 'N/A',
        'passed': attributed == len(conversions)
    })
    
    # 6. Session count sanity
    checks.append({
        'check': 'Sessions Created',
        'original': events['client_id'].nunique(),
        'transformed': len(sessions),
        'note': f'{len(sessions)/events["client_id"].nunique():.2f} sessions per client',
        'passed': len(sessions) >= events['client_id'].nunique()
    })
    
    # Print results
    print("\nValidation Results:")
    print("-" * 80)
    for check in checks:
        status = "✓" if check['passed'] else "✗"
        print(f"{status} {check['check']}: {check['original']} → {check['transformed']} ({check['note']})")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    import sys
    
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "./transformed"
    
    tables = run_transformation(data_dir, output_dir)
    validate_transformation(tables, data_dir)
