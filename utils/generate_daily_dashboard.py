#!/usr/bin/env python3
"""
Daily Reconciliation Dashboard Generator
Creates summary reports for daily reconciliation runs
"""

import pandas as pd
import pyodbc
from datetime import datetime, date, timedelta
import logging
from pathlib import Path
from reconciliation_utils import get_connection
from tabulate import tabulate

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DailyDashboardGenerator:
    def __init__(self):
        self.connection = get_connection()
        self.reports_dir = Path("reports/daily_dashboards")
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
    def get_recent_batches(self, days=7):
        """Get recent reconciliation batches"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = """
        SELECT 
            batch_id,
            name,
            start_time,
            end_time,
            status,
            matched_count,
            unmatched_count,
            fuzzy_threshold,
            DATEDIFF(second, start_time, end_time) as duration_seconds
        FROM reconciliation_batches 
        WHERE start_time >= ?
        ORDER BY start_time DESC
        """
        
        return pd.read_sql(query, self.connection, params=[cutoff_date])
    
    def get_customer_performance(self, days=30):
        """Get customer performance summary"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = """
        SELECT 
            rb.name as batch_name,
            COUNT(rr.id) as total_shipments,
            SUM(CASE WHEN rr.reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) as matched,
            SUM(CASE WHEN rr.reconciliation_status = 'UNMATCHED' THEN 1 ELSE 0 END) as unmatched,
            AVG(CASE WHEN rr.match_confidence IS NOT NULL THEN rr.match_confidence ELSE 0 END) as avg_confidence,
            rb.start_time
        FROM reconciliation_batches rb
        LEFT JOIN reconciliation_results rr ON rb.batch_id = rr.batch_id
        WHERE rb.start_time >= ?
        GROUP BY rb.name, rb.batch_id, rb.start_time
        ORDER BY rb.start_time DESC
        """
        
        return pd.read_sql(query, self.connection, params=[cutoff_date])
    
    def get_shipment_summary(self, days=7):
        """Get shipment volume summary"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = """
        SELECT 
            customer_name,
            ship_date,
            COUNT(*) as shipment_count,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT style_code) as unique_styles
        FROM stg_fm_orders_shipped_table
        WHERE ship_date >= ?
        GROUP BY customer_name, ship_date
        ORDER BY ship_date DESC, customer_name
        """
        
        return pd.read_sql(query, self.connection, params=[cutoff_date])
    
    def generate_daily_summary(self, target_date=None):
        """Generate daily summary report"""
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"Generating daily summary for {target_date}")
        
        # Get data for the day
        batches_df = self.get_recent_batches(days=1)
        customer_perf_df = self.get_customer_performance(days=1)
        shipments_df = self.get_shipment_summary(days=1)
        
        # Filter for target date
        target_str = target_date.strftime('%Y-%m-%d')
        batches_today = batches_df[batches_df['start_time'].dt.date == target_date] if not batches_df.empty else pd.DataFrame()
        
        report_content = f"""# Daily Reconciliation Summary
**Date:** {target_date.strftime('%Y-%m-%d')}  
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview
"""
        
        if batches_today.empty:
            report_content += "❌ No reconciliation batches run today\n\n"
        else:
            total_matched = batches_today['matched_count'].sum()
            total_unmatched = batches_today['unmatched_count'].sum()
            total_processed = total_matched + total_unmatched
            success_rate = (total_matched / total_processed * 100) if total_processed > 0 else 0
            
            report_content += f"""
| Metric | Value |
|--------|-------|
| **Batches Run** | {len(batches_today)} |
| **Total Processed** | {total_processed:,} |
| **Successfully Matched** | {total_matched:,} |
| **Unmatched** | {total_unmatched:,} |
| **Success Rate** | {success_rate:.1f}% |

## Batch Details

{batches_today[['name', 'status', 'matched_count', 'unmatched_count', 'duration_seconds']].to_markdown(index=False)}

"""

        # Add shipment volume
        shipments_today = shipments_df[shipments_df['ship_date'].dt.date == target_date] if not shipments_df.empty else pd.DataFrame()
        
        if not shipments_today.empty:
            report_content += f"""
## Shipment Volume Today

{shipments_today.to_markdown(index=False)}

**Total Shipments:** {shipments_today['shipment_count'].sum():,}  
**Total Quantity:** {shipments_today['total_quantity'].sum():,}  
**Active Customers:** {shipments_today['customer_name'].nunique()}
"""

        # Save report
        report_path = self.reports_dir / f"daily_summary_{target_date.strftime('%Y%m%d')}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Daily summary saved: {report_path}")
        return report_path
    
    def generate_weekly_summary(self, end_date=None):
        """Generate weekly summary report"""
        if end_date is None:
            end_date = date.today()
        
        start_date = end_date - timedelta(days=6)
        
        logger.info(f"Generating weekly summary for {start_date} to {end_date}")
        
        batches_df = self.get_recent_batches(days=7)
        customer_perf_df = self.get_customer_performance(days=7)
        
        # Weekly aggregation
        if not batches_df.empty:
            batches_df['date'] = batches_df['start_time'].dt.date
            weekly_summary = batches_df.groupby('date').agg({
                'batch_id': 'count',
                'matched_count': 'sum',
                'unmatched_count': 'sum',
                'duration_seconds': 'mean'
            }).round(1)
            weekly_summary.columns = ['Batches', 'Matched', 'Unmatched', 'Avg_Duration_Sec']
            weekly_summary['Success_Rate'] = (weekly_summary['Matched'] / 
                                            (weekly_summary['Matched'] + weekly_summary['Unmatched']) * 100).round(1)
        
        report_content = f"""# Weekly Reconciliation Summary
**Period:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}  
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Weekly Performance
"""

        if batches_df.empty:
            report_content += "❌ No reconciliation activity this week\n"
        else:
            total_batches = len(batches_df)
            total_matched = batches_df['matched_count'].sum()
            total_unmatched = batches_df['unmatched_count'].sum()
            overall_success = (total_matched / (total_matched + total_unmatched) * 100) if (total_matched + total_unmatched) > 0 else 0
            
            report_content += f"""
### Summary
| Metric | Value |
|--------|-------|
| **Total Batches** | {total_batches} |
| **Total Matched** | {total_matched:,} |
| **Total Unmatched** | {total_unmatched:,} |
| **Overall Success Rate** | {overall_success:.1f}% |

### Daily Breakdown
{weekly_summary.to_markdown()}

"""

        # Customer performance
        if not customer_perf_df.empty:
            customer_summary = customer_perf_df.groupby('batch_name').agg({
                'total_shipments': 'sum',
                'matched': 'sum',
                'unmatched': 'sum',
                'avg_confidence': 'mean'
            }).round(2)
            customer_summary['Success_Rate'] = (customer_summary['matched'] / 
                                              customer_summary['total_shipments'] * 100).round(1)
            
            report_content += f"""
## Customer Performance
{customer_summary.to_markdown()}
"""

        # Save report
        report_path = self.reports_dir / f"weekly_summary_{end_date.strftime('%Y%m%d')}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Weekly summary saved: {report_path}")
        return report_path
    
    def generate_customer_focus_report(self, customer, days=30):
        """Generate customer-specific performance report"""
        logger.info(f"Generating customer focus report for {customer}")
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Customer-specific reconciliation results
        query = """
        SELECT 
            rb.name,
            rb.start_time,
            rb.status,
            COUNT(rr.id) as total_records,
            SUM(CASE WHEN rr.reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) as matched,
            SUM(CASE WHEN rr.reconciliation_status = 'UNMATCHED' THEN 1 ELSE 0 END) as unmatched,
            AVG(rr.match_confidence) as avg_confidence,
            MIN(rr.match_confidence) as min_confidence,
            MAX(rr.match_confidence) as max_confidence
        FROM reconciliation_batches rb
        LEFT JOIN reconciliation_results rr ON rb.batch_id = rr.batch_id
        WHERE rb.name LIKE ? AND rb.start_time >= ?
        GROUP BY rb.name, rb.batch_id, rb.start_time, rb.status
        ORDER BY rb.start_time DESC
        """
        
        customer_results = pd.read_sql(query, self.connection, params=[f"%{customer}%", cutoff_date])
        
        report_content = f"""# Customer Focus Report: {customer}
**Period:** Last {days} days  
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""

        if customer_results.empty:
            report_content += f"❌ No reconciliation activity found for {customer} in the last {days} days\n"
        else:
            # Overall stats
            total_records = customer_results['total_records'].sum()
            total_matched = customer_results['matched'].sum()
            total_unmatched = customer_results['unmatched'].sum()
            overall_success = (total_matched / total_records * 100) if total_records > 0 else 0
            avg_confidence = customer_results['avg_confidence'].mean()
            
            report_content += f"""
## Performance Summary
| Metric | Value |
|--------|-------|
| **Total Reconciliation Runs** | {len(customer_results)} |
| **Total Records Processed** | {total_records:,} |
| **Successfully Matched** | {total_matched:,} |
| **Unmatched** | {total_unmatched:,} |
| **Success Rate** | {overall_success:.1f}% |
| **Average Confidence** | {avg_confidence:.1f}% |

## Recent Activity
{customer_results[['name', 'start_time', 'status', 'matched', 'unmatched', 'avg_confidence']].to_markdown(index=False)}

"""

        # Add trends and recommendations
        if not customer_results.empty and len(customer_results) >= 3:
            # Calculate trend
            recent_3 = customer_results.head(3)
            older_3 = customer_results.tail(3)
            
            recent_success = (recent_3['matched'].sum() / recent_3['total_records'].sum() * 100) if recent_3['total_records'].sum() > 0 else 0
            older_success = (older_3['matched'].sum() / older_3['total_records'].sum() * 100) if older_3['total_records'].sum() > 0 else 0
            
            trend = recent_success - older_success
            
            report_content += f"""
## Trend Analysis
"""
            if trend > 5:
                report_content += f"📈 **Improving** - Success rate increased by {trend:.1f}% in recent runs\n"
            elif trend < -5:
                report_content += f"📉 **Declining** - Success rate decreased by {abs(trend):.1f}% in recent runs\n"
            else:
                report_content += f"➡️ **Stable** - Success rate consistent (±{abs(trend):.1f}%)\n"

        # Save report
        report_path = self.reports_dir / f"customer_focus_{customer}_{datetime.now().strftime('%Y%m%d')}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Customer focus report saved: {report_path}")
        return report_path

def main():
    """Generate all dashboard reports"""
    dashboard = DailyDashboardGenerator()
    
    # Generate today's summary
    dashboard.generate_daily_summary()
    
    # Generate weekly summary
    dashboard.generate_weekly_summary()
    
    # Generate customer focus report for GREYSON
    dashboard.generate_customer_focus_report("GREYSON")
    
    logger.info("✅ All dashboard reports generated!")

if __name__ == "__main__":
    main()
