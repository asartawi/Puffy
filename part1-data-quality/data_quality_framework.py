"""
Puffy Data Quality Validation Framework
========================================
A comprehensive framework for validating raw event data before it enters 
production analytics pipelines.

Author: Abdulhafeth Salah
Date: 2025
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity levels for data quality issues"""
    CRITICAL = "CRITICAL"  # Blocks pipeline, data unusable
    HIGH = "HIGH"          # Significant impact on analytics accuracy
    MEDIUM = "MEDIUM"      # May cause partial data loss or inaccuracy
    LOW = "LOW"            # Minor issue, informational


@dataclass
class ValidationResult:
    """Result of a single validation check"""
    check_name: str
    passed: bool
    severity: Severity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    affected_rows: int = 0
    total_rows: int = 0
    
    @property
    def failure_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return self.affected_rows / self.total_rows


@dataclass 
class DataQualityReport:
    """Complete data quality report for a file or batch"""
    file_name: str
    file_date: str
    total_rows: int
    validation_results: List[ValidationResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        """Returns True if no CRITICAL issues found"""
        return not any(
            r.severity == Severity.CRITICAL and not r.passed 
            for r in self.validation_results
        )
    
    @property
    def critical_issues(self) -> List[ValidationResult]:
        return [r for r in self.validation_results if r.severity == Severity.CRITICAL and not r.passed]
    
    @property
    def high_issues(self) -> List[ValidationResult]:
        return [r for r in self.validation_results if r.severity == Severity.HIGH and not r.passed]
    
    def summary(self) -> str:
        """Generate human-readable summary"""
        lines = [
            f"\n{'='*60}",
            f"DATA QUALITY REPORT: {self.file_name}",
            f"{'='*60}",
            f"Date: {self.file_date}",
            f"Total Rows: {self.total_rows:,}",
            f"Overall Status: {'✓ PASSED' if self.passed else '✗ FAILED'}",
            f"",
            "VALIDATION RESULTS:",
            "-" * 40
        ]
        
        for result in self.validation_results:
            status = "✓" if result.passed else "✗"
            lines.append(f"{status} [{result.severity.value}] {result.check_name}")
            if not result.passed:
                lines.append(f"   → {result.message}")
                if result.affected_rows > 0:
                    lines.append(f"   → Affected: {result.affected_rows:,} rows ({result.failure_rate:.1%})")
        
        return "\n".join(lines)


class DataQualityValidator:
    """
    Main validation framework for Puffy event data.
    
    Validates:
    - Schema consistency
    - Data completeness
    - Data integrity
    - Business logic
    - Temporal consistency
    """
    
    EXPECTED_COLUMNS = ['client_id', 'page_url', 'referrer', 'timestamp', 'event_name', 'event_data', 'user_agent']
    VALID_EVENT_NAMES = ['page_viewed', 'email_filled_on_popup', 'product_added_to_cart', 'checkout_started', 'checkout_completed', 'purchase']
    VALID_DOMAINS = ['puffy.com', 'checkout.puffy.com']
    
    # Thresholds for anomaly detection (based on historical patterns)
    NULL_CLIENT_ID_THRESHOLD = 0.05  # 5% - anything above is anomalous
    MIN_DAILY_EVENTS = 1000
    MAX_DAILY_EVENTS = 10000
    EXPECTED_PURCHASE_RATE = (0.003, 0.02)  # 0.3% - 2% of events
    
    def __init__(self, historical_stats: Optional[Dict] = None):
        """
        Initialize validator with optional historical statistics for comparison.
        
        Args:
            historical_stats: Dict with keys like 'avg_daily_events', 'avg_revenue', etc.
        """
        self.historical_stats = historical_stats or {}
        self.known_transaction_ids = set()  # For cross-file duplicate detection
    
    def validate_file(self, file_path: str) -> DataQualityReport:
        """
        Run all validations on a single event file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataQualityReport with all validation results
        """
        file_name = os.path.basename(file_path)
        file_date = self._extract_date_from_filename(file_name)
        
        logger.info(f"Validating file: {file_name}")
        
        # Load data
        df = pd.read_csv(file_path, dtype=str)
        
        report = DataQualityReport(
            file_name=file_name,
            file_date=file_date,
            total_rows=len(df)
        )
        
        # Run all validations
        validations = [
            self._validate_schema(df),
            self._validate_client_id_completeness(df),
            self._validate_timestamp_format(df),
            self._validate_timestamp_date_match(df, file_date),
            self._validate_event_names(df),
            self._validate_page_url_format(df),
            self._validate_event_data_json(df),
            self._validate_purchase_events(df),
            self._validate_duplicate_transactions(df),
            self._validate_event_volume(df),
            self._validate_funnel_integrity(df),
            self._validate_referrer_presence(df),
        ]
        
        report.validation_results = validations
        return report
    
    def validate_batch(self, file_paths: List[str]) -> Tuple[List[DataQualityReport], Dict]:
        """
        Validate multiple files and perform cross-file checks.
        
        Args:
            file_paths: List of paths to CSV files
            
        Returns:
            Tuple of (list of reports, cross-file analysis dict)
        """
        reports = []
        all_transactions = []
        daily_metrics = []
        
        for file_path in sorted(file_paths):
            report = self.validate_file(file_path)
            reports.append(report)
            
            # Collect metrics for cross-file analysis
            df = pd.read_csv(file_path, dtype=str)
            if 'clientId' in df.columns:
                df = df.rename(columns={'clientId': 'client_id'})
            
            metrics = self._extract_daily_metrics(df, report.file_date)
            daily_metrics.append(metrics)
            
            # Collect transactions for duplicate check
            transactions = self._extract_transactions(df)
            all_transactions.extend(transactions)
        
        # Cross-file analysis
        cross_file_analysis = {
            'duplicate_transactions': self._find_cross_file_duplicates(all_transactions),
            'trend_anomalies': self._detect_trend_anomalies(daily_metrics),
            'schema_changes': self._detect_schema_changes(reports)
        }
        
        return reports, cross_file_analysis
    
    # =========================================================================
    # SCHEMA VALIDATIONS
    # =========================================================================
    
    def _validate_schema(self, df: pd.DataFrame) -> ValidationResult:
        """Check that all expected columns are present"""
        # Normalize column name for comparison
        # we already know that the files have column naming issue for the column client_id, so we are going to fix it and take notes
        columns = [c.lower().replace('clientid', 'client_id') for c in df.columns]
        
        missing = set(self.EXPECTED_COLUMNS) - set(columns)
        extra = set(columns) - set(self.EXPECTED_COLUMNS)
        
        if missing:
            return ValidationResult(
                check_name="Schema - Required Columns",
                passed=False,
                severity=Severity.CRITICAL,
                message=f"Missing columns: {missing}",
                details={'missing': list(missing), 'extra': list(extra)},
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Schema - Required Columns",
            passed=True,
            severity=Severity.CRITICAL,
            message="All required columns present",
            total_rows=len(df)
        )
    
    
    def _validate_referrer_presence(self, df: pd.DataFrame) -> ValidationResult:
        """Check that referrer column exists (critical for attribution)"""
        if 'referrer' not in df.columns:
            return ValidationResult(
                check_name="Schema - Referrer Column Present",
                passed=False,
                severity=Severity.CRITICAL,
                message="Referrer column is MISSING - attribution will be broken",
                details={'columns_present': list(df.columns)},
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Schema - Referrer Column Present",
            passed=True,
            severity=Severity.CRITICAL,
            message="Referrer column present",
            total_rows=len(df)
        )
    
    # =========================================================================
    # DATA COMPLETENESS VALIDATIONS
    # =========================================================================
    
    def _validate_client_id_completeness(self, df: pd.DataFrame) -> ValidationResult:
        """Check for NULL/empty client_id values"""
        # Normalize column nameو we know the the column name is not named well in some files so lets fix it and test another aspect
        client_col = 'clientId' if 'clientId' in df.columns else 'client_id'
        
        null_count = df[client_col].isna().sum() + (df[client_col] == '').sum()
        null_rate = null_count / len(df)
        
        if null_rate > self.NULL_CLIENT_ID_THRESHOLD:
            return ValidationResult(
                check_name="Completeness - Client ID",
                passed=False,
                severity=Severity.CRITICAL,
                message=f"NULL client_id rate ({null_rate:.1%}) exceeds threshold ({self.NULL_CLIENT_ID_THRESHOLD:.1%})",
                details={'null_rate': null_rate, 'threshold': self.NULL_CLIENT_ID_THRESHOLD},
                affected_rows=null_count,
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Completeness - Client ID",
            passed=True,
            severity=Severity.CRITICAL,
            message=f"NULL client_id rate ({null_rate:.1%}) within acceptable range",
            affected_rows=null_count,
            total_rows=len(df)
        )
    
    # =========================================================================
    # TIMESTAMP VALIDATIONS
    # =========================================================================
    
    def _validate_timestamp_format(self, df: pd.DataFrame) -> ValidationResult:
        """Validate ISO 8601 timestamp format"""
        iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$'
        
        invalid_mask = ~df['timestamp'].str.match(iso_pattern, na=True)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            return ValidationResult(
                check_name="Format - Timestamp ISO 8601",
                passed=False,
                severity=Severity.HIGH,
                message=f"Invalid timestamp format found",
                details={'sample_invalid': df[invalid_mask]['timestamp'].head(5).tolist()},
                affected_rows=invalid_count,
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Format - Timestamp ISO 8601",
            passed=True,
            severity=Severity.HIGH,
            message="All timestamps in valid ISO 8601 format",
            total_rows=len(df)
        )
    
    def _validate_timestamp_date_match(self, df: pd.DataFrame, expected_date: str) -> ValidationResult:
        """Ensure event timestamps match the file's date partition"""
        expected_prefix = f"{expected_date[:4]}-{expected_date[4:6]}-{expected_date[6:]}"
        
        mismatched = ~df['timestamp'].str.startswith(expected_prefix, na=True)
        mismatched_count = mismatched.sum()
        
        if mismatched_count > 0:
            return ValidationResult(
                check_name="Integrity - Timestamp Date Partition Match",
                passed=False,
                severity=Severity.HIGH,
                message=f"Events found with timestamps not matching file date ({expected_prefix})",
                details={'expected_date': expected_prefix, 'sample_mismatched': df[mismatched]['timestamp'].head(5).tolist()},
                affected_rows=mismatched_count,
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Integrity - Timestamp Date Partition Match",
            passed=True,
            severity=Severity.HIGH,
            message="All timestamps match file date partition",
            total_rows=len(df)
        )
    
    # =========================================================================
    # EVENT VALIDATIONS
    # =========================================================================
    
    def _validate_event_names(self, df: pd.DataFrame) -> ValidationResult:
        """Check that all event_name values are valid"""
        invalid_events = df[~df['event_name'].isin(self.VALID_EVENT_NAMES)]
        invalid_count = len(invalid_events)
        
        if invalid_count > 0:
            invalid_values = invalid_events['event_name'].unique().tolist()
            return ValidationResult(
                check_name="Integrity - Event Name Values",
                passed=False,
                severity=Severity.CRITICAL,
                message=f"Invalid event names found: {invalid_values[:5]}",
                details={'invalid_values': invalid_values, 'valid_values': self.VALID_EVENT_NAMES},
                affected_rows=invalid_count,
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Integrity - Event Name Values",
            passed=True,
            severity=Severity.CRITICAL,
            message="All event names are valid",
            total_rows=len(df)
        )
    
    def _validate_page_url_format(self, df: pd.DataFrame) -> ValidationResult:
        """Validate page_url contains valid Puffy domains"""
        valid_pattern = r'^https://(puffy\.com|checkout\.puffy\.com)'
        
        invalid_mask = ~df['page_url'].str.match(valid_pattern, na=True)
        invalid_count = invalid_mask.sum()
        
        # This is informational - checkout.puffy.com is valid
        if invalid_count > 0:
            sample = df[invalid_mask]['page_url'].head(5).tolist()
            # Check if they're all checkout URLs (acceptable)
            checkout_pattern = r'^https://checkout\.puffy\.com'
            all_checkout = all(re.match(checkout_pattern, url) if url else False for url in sample)
            
            if not all_checkout:
                return ValidationResult(
                    check_name="Integrity - Page URL Domain",
                    passed=False,
                    severity=Severity.MEDIUM,
                    message="Non-Puffy domain URLs found",
                    details={'sample_invalid': sample},
                    affected_rows=invalid_count,
                    total_rows=len(df)
                )
        
        return ValidationResult(
            check_name="Integrity - Page URL Domain",
            passed=True,
            severity=Severity.MEDIUM,
            message="All URLs from valid Puffy domains",
            total_rows=len(df)
        )
    
    def _validate_event_data_json(self, df: pd.DataFrame) -> ValidationResult:
        """Validate that event_data contains valid JSON when present"""
        has_data = df[df['event_data'].notna() & (df['event_data'] != '')]
        
        invalid_count = 0
        invalid_samples = []
        
        for idx, row in has_data.iterrows():
            try:
                json.loads(row['event_data'])
            except (json.JSONDecodeError, TypeError):
                invalid_count += 1
                if len(invalid_samples) < 3:
                    invalid_samples.append(str(row['event_data'])[:100])
        
        if invalid_count > 0:
            return ValidationResult(
                check_name="Format - Event Data JSON",
                passed=False,
                severity=Severity.HIGH,
                message=f"Invalid JSON in event_data",
                details={'sample_invalid': invalid_samples},
                affected_rows=invalid_count,
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Format - Event Data JSON",
            passed=True,
            severity=Severity.HIGH,
            message="All event_data is valid JSON",
            total_rows=len(df)
        )
    
    # =========================================================================
    # PURCHASE/REVENUE VALIDATIONS
    # =========================================================================
    
    def _validate_purchase_events(self, df: pd.DataFrame) -> ValidationResult:
        """Validate purchase event data integrity"""
        purchases = df[df['event_name'].isin(['checkout_completed', 'purchase'])]
        
        if len(purchases) == 0:
            return ValidationResult(
                check_name="Business Logic - Purchase Events",
                passed=True,
                severity=Severity.MEDIUM,
                message="No purchase events in file",
                total_rows=len(df)
            )
        
        issues = []
        zero_revenue = 0
        negative_revenue = 0
        missing_transaction_id = 0
        null_item_prices = 0
        
        for idx, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                
                # Check transaction_id
                if 'transaction_id' not in data or not data['transaction_id']:
                    missing_transaction_id += 1
                
                # Check revenue
                revenue = data.get('revenue')
                if revenue is not None:
                    revenue = float(revenue)
                    if revenue == 0:
                        zero_revenue += 1
                    if revenue < 0:
                        negative_revenue += 1
                
                # Check item prices
                items = data.get('items', [])
                for item in items:
                    if item.get('item_price') is None:
                        null_item_prices += 1
                        
            except (json.JSONDecodeError, TypeError, ValueError):
                issues.append("Invalid event_data JSON")
        
        all_issues = []
        if zero_revenue > 0:
            all_issues.append(f"$0 revenue: {zero_revenue}")
        if negative_revenue > 0:
            all_issues.append(f"Negative revenue: {negative_revenue}")
        if missing_transaction_id > 0:
            all_issues.append(f"Missing transaction_id: {missing_transaction_id}")
        if null_item_prices > 0:
            all_issues.append(f"NULL item_prices: {null_item_prices}")
        
        if all_issues:
            return ValidationResult(
                check_name="Business Logic - Purchase Events",
                passed=False,
                severity=Severity.HIGH,
                message="; ".join(all_issues),
                details={
                    'zero_revenue': zero_revenue,
                    'negative_revenue': negative_revenue,
                    'missing_transaction_id': missing_transaction_id,
                    'null_item_prices': null_item_prices
                },
                affected_rows=len(purchases),
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Business Logic - Purchase Events",
            passed=True,
            severity=Severity.HIGH,
            message=f"All {len(purchases)} purchase events valid",
            total_rows=len(df)
        )
    
    def _validate_duplicate_transactions(self, df: pd.DataFrame) -> ValidationResult:
        """Check for duplicate transaction IDs within the file"""
        purchases = df[df['event_name'].isin(['checkout_completed', 'purchase'])]
        
        transaction_ids = []
        for idx, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                tid = data.get('transaction_id')
                if tid:
                    transaction_ids.append(tid)
            except:
                pass
        
        if not transaction_ids:
            return ValidationResult(
                check_name="Integrity - Duplicate Transactions",
                passed=True,
                severity=Severity.CRITICAL,
                message="No transactions to check",
                total_rows=len(df)
            )
        
        from collections import Counter
        counts = Counter(transaction_ids)
        duplicates = {k: v for k, v in counts.items() if v > 1}
        
        if duplicates:
            return ValidationResult(
                check_name="Integrity - Duplicate Transactions",
                passed=False,
                severity=Severity.CRITICAL,
                message=f"Duplicate transaction IDs found: {list(duplicates.keys())}",
                details={'duplicates': duplicates},
                affected_rows=sum(duplicates.values()),
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Integrity - Duplicate Transactions",
            passed=True,
            severity=Severity.CRITICAL,
            message="No duplicate transaction IDs",
            total_rows=len(df)
        )
    
    # =========================================================================
    # VOLUME AND TREND VALIDATIONS
    # =========================================================================
    
    def _validate_event_volume(self, df: pd.DataFrame) -> ValidationResult:
        """Check that event volume is within expected range"""
        row_count = len(df)
        
        if row_count < self.MIN_DAILY_EVENTS:
            return ValidationResult(
                check_name="Volume - Daily Event Count",
                passed=False,
                severity=Severity.HIGH,
                message=f"Event count ({row_count:,}) below minimum ({self.MIN_DAILY_EVENTS:,})",
                details={'count': row_count, 'min': self.MIN_DAILY_EVENTS},
                total_rows=row_count
            )
        
        if row_count > self.MAX_DAILY_EVENTS:
            return ValidationResult(
                check_name="Volume - Daily Event Count",
                passed=False,
                severity=Severity.MEDIUM,
                message=f"Event count ({row_count:,}) above maximum ({self.MAX_DAILY_EVENTS:,})",
                details={'count': row_count, 'max': self.MAX_DAILY_EVENTS},
                total_rows=row_count
            )
        
        return ValidationResult(
            check_name="Volume - Daily Event Count",
            passed=True,
            severity=Severity.HIGH,
            message=f"Event count ({row_count:,}) within expected range",
            total_rows=row_count
        )
    
    def _validate_funnel_integrity(self, df: pd.DataFrame) -> ValidationResult:
        """Check that conversion events have corresponding funnel events"""
        # Normalize column name
        client_col = 'clientId' if 'clientId' in df.columns else 'client_id'
        
        page_view_clients = set(df[df['event_name'] == 'page_viewed'][client_col].dropna())
        purchase_clients = set(df[df['event_name'] == 'checkout_completed'][client_col].dropna())
        
        orphan_purchases = purchase_clients - page_view_clients
        orphan_rate = len(orphan_purchases) / len(purchase_clients) if purchase_clients else 0
        
        # Some orphans are expected (cross-session purchases)
        if orphan_rate > 0.5:  # More than 50% orphans is suspicious
            return ValidationResult(
                check_name="Business Logic - Funnel Integrity",
                passed=False,
                severity=Severity.MEDIUM,
                message=f"High rate ({orphan_rate:.1%}) of purchases without same-day page views",
                details={'orphan_count': len(orphan_purchases), 'total_purchases': len(purchase_clients)},
                affected_rows=len(orphan_purchases),
                total_rows=len(df)
            )
        
        return ValidationResult(
            check_name="Business Logic - Funnel Integrity",
            passed=True,
            severity=Severity.MEDIUM,
            message=f"Funnel integrity OK ({len(orphan_purchases)} orphan purchases)",
            total_rows=len(df)
        )
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _extract_date_from_filename(self, filename: str) -> str:
        """Extract date from filename like events_20250223.csv"""
        match = re.search(r'(\d{8})', filename)
        return match.group(1) if match else ""
    
    def _extract_daily_metrics(self, df: pd.DataFrame, date: str) -> Dict:
        """Extract key metrics for trend analysis"""
        # Normalize column name
        if 'clientId' in df.columns:
            df = df.rename(columns={'clientId': 'client_id'})
            
        purchases = df[df['event_name'] == 'checkout_completed']
        total_revenue = 0
        for idx, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                total_revenue += float(data.get('revenue', 0))
            except:
                pass
        
        null_client_rate = (df['client_id'].isna().sum() + (df['client_id'] == '').sum()) / len(df)
        
        return {
            'date': date,
            'total_events': len(df),
            'purchases': len(purchases),
            'revenue': total_revenue,
            'null_client_rate': null_client_rate,
            'has_referrer': 'referrer' in df.columns
        }
    
    def _extract_transactions(self, df: pd.DataFrame) -> List[Dict]:
        """Extract transaction data for cross-file duplicate detection"""
        purchases = df[df['event_name'].isin(['checkout_completed', 'purchase'])]
        transactions = []
        
        for idx, row in purchases.iterrows():
            try:
                data = json.loads(row['event_data'])
                transactions.append({
                    'transaction_id': data.get('transaction_id'),
                    'revenue': data.get('revenue'),
                    'timestamp': row['timestamp']
                })
            except:
                pass
        
        return transactions
    
    def _find_cross_file_duplicates(self, all_transactions: List[Dict]) -> List[Dict]:
        """Find transactions that appear in multiple files"""
        from collections import Counter
        
        tid_counts = Counter(t['transaction_id'] for t in all_transactions if t['transaction_id'])
        duplicates = []
        
        for tid, count in tid_counts.items():
            if count > 1:
                # Find all occurrences
                occurrences = [t for t in all_transactions if t['transaction_id'] == tid]
                duplicates.append({
                    'transaction_id': tid,
                    'count': count,
                    'occurrences': occurrences
                })
        
        return duplicates
    
    def _detect_trend_anomalies(self, daily_metrics: List[Dict]) -> List[str]:
        """Detect anomalies in daily trends"""
        anomalies = []
        
        if len(daily_metrics) < 2:
            return anomalies
        
        # Check for sudden changes in null_client_rate
        for i in range(1, len(daily_metrics)):
            prev = daily_metrics[i-1]
            curr = daily_metrics[i]
            
            # Null rate spike
            if curr['null_client_rate'] > prev['null_client_rate'] * 3 and curr['null_client_rate'] > 0.05:
                anomalies.append(
                    f"NULL client_id spike on {curr['date']}: "
                    f"{prev['null_client_rate']:.1%} → {curr['null_client_rate']:.1%}"
                )
            
            # Revenue drop
            if prev['revenue'] > 0 and curr['revenue'] < prev['revenue'] * 0.3:
                anomalies.append(
                    f"Revenue drop on {curr['date']}: "
                    f"${prev['revenue']:,.0f} → ${curr['revenue']:,.0f}"
                )
            
            # Schema change (referrer disappeared)
            if prev['has_referrer'] and not curr['has_referrer']:
                anomalies.append(
                    f"Referrer column MISSING starting {curr['date']}"
                )
        
        return anomalies
    
    def _detect_schema_changes(self, reports: List[DataQualityReport]) -> List[str]:
        """Detect schema changes across files"""
        changes = []
        # Schema changes are already captured in individual validations
        return changes


def run_validation(data_dir: str) -> None:
    """
    Run validation on all event files in a directory.
    
    Args:
        data_dir: Path to directory containing event CSV files
    """
    validator = DataQualityValidator()
    
    # Find all event files
    files = sorted([
        os.path.join(data_dir, f) 
        for f in os.listdir(data_dir) 
        if f.startswith('events_') and f.endswith('.csv')
    ])
    
    if not files:
        print(f"No event files found in {data_dir}")
        return
    
    print(f"\nFound {len(files)} event files to validate\n")
    
    # Run batch validation
    reports, cross_file = validator.validate_batch(files)
    
    # Lets print the reports of each file alone
    for report in reports:
        print(report.summary())
    
    # EXTRA: Print cross-file analysis
    print("\n" + "="*60)
    print("CROSS-FILE ANALYSIS")
    print("="*60)
    
    if cross_file['duplicate_transactions']:
        print(f"\n⚠️ DUPLICATE TRANSACTIONS ACROSS FILES:")
        for dup in cross_file['duplicate_transactions']:
            print(f"   - {dup['transaction_id']}: {dup['count']} occurrences")
    else:
        print("\n✓ No cross-file duplicate transactions")
    
    if cross_file['trend_anomalies']:
        print(f"\n⚠️ TREND ANOMALIES DETECTED:")
        for anomaly in cross_file['trend_anomalies']:
            print(f"   - {anomaly}")
    else:
        print("\n✓ No trend anomalies detected")
    
    # Summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    failed_files = [r for r in reports if not r.passed]
    print(f"\nTotal files: {len(reports)}")
    print(f"Passed: {len(reports) - len(failed_files)}")
    print(f"Failed: {len(failed_files)}")
    
    if failed_files:
        print("\nFailed files:")
        for r in failed_files:
            critical = [i.check_name for i in r.critical_issues]
            print(f"  - {r.file_name}: {', '.join(critical)}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    else:
        # Default to current directory
        data_dir = "."
    
    run_validation(data_dir)
