#!/usr/bin/env python3
"""
Nifty 100 Weekly 52-Week High Detector
=====================================

This script checks Nifty 100 stocks for 52-week highs from Monday to Friday 3 PM.
It can be scheduled to run automatically every Friday at 3 PM.

Usage:
    python nifty_52w_high_detector.py
    python nifty_52w_high_detector.py --weeks-back 4  # Check last 4 weeks
    python nifty_52w_high_detector.py --output csv    # Save to CSV
    python nifty_52w_high_detector.py --email your@email.com  # Email results

Dependencies:
    pip install yfinance pandas numpy openpyxl

Author: Tanmay Chandane
Date: 28 September 2025
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import argparse
import warnings
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

class Nifty100WeeklyHighDetector:
    """
    Advanced system to detect Nifty 100 stocks hitting 52-week highs during the trading week
    """

    def __init__(self):
        # Complete Nifty 100 stocks list (updated for September 2025)
        self.nifty100_stocks = [
            'ABB', 'ADANIENSOL', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'AMBUJACEM',
            'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJAJHFL', 'BAJAJHLDNG', 'BAJFINANCE',
            'BANKBARODA', 'BEL', 'BHARTIARTL', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'CANBK',
            'CGPOWER', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'DABUR', 'DIVISLAB', 'DLF', 'DMART',
            'DRREDDY', 'EICHERMOT', 'ETERNAL', 'GAIL', 'GODREJCP', 'GRASIM', 'HAL',
            'HAVELLS', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR',
            'HYUNDAI', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'INDHOTEL', 'INDIGO',
            'INDUSINDBK', 'INFY', 'IOC', 'IRFC', 'ITC', 'JINDALSTEL', 'JIOFIN',
            'JSWENERGY', 'JSWSTEEL', 'KOTAKBANK', 'LICI', 'LODHA', 'LT', 'LTIM',
            'M&M', 'MARUTI', 'MOTHERSON', 'NAUKRI', 'NESTLEIND', 'NTPC', 'ONGC', 'PFC',
            'PIDILITIND', 'PNB', 'POWERGRID', 'RECLTD', 'RELIANCE', 'SBILIFE', 'SBIN', 'SHREECEM',
            'SHRIRAMFIN', 'SIEMENS', 'SUNPHARMA', 'SWIGGY', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL',
            'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'TRENT', 'TVSMOTOR', 'ULTRACEMCO', 'UNITDSPR',
            'VBL', 'VEDL', 'WIPRO', 'ZYDUSLIFE'
        ]

        # Limit to exactly 100 stocks and add .NS suffix for yfinance
        self.nifty100_stocks = self.nifty100_stocks[:100]
        self.nifty100_with_ns = [stock + '.NS' for stock in self.nifty100_stocks]

        logger.info(f"Initialized with {len(self.nifty100_stocks)} Nifty 100 stocks")

    def get_week_dates(self, weeks_back=0):
        """Get Monday and Friday of the target week"""
        today = datetime.now().date()

        # Calculate the Monday of the target week
        days_since_monday = today.weekday()
        this_monday = today - timedelta(days=days_since_monday)
        target_monday = this_monday - timedelta(weeks=weeks_back)

        # Calculate Friday of the same week
        target_friday = target_monday + timedelta(days=4)

        return target_monday, target_friday

    def is_52_week_high(self, ticker_data, current_high, check_date, tolerance=0.001):
        """
        Check if current high is a 52-week high

        Args:
            ticker_data: Historical data for the stock
            current_high: The high price to check
            check_date: Date to check against
            tolerance: Tolerance for considering a price as 52-week high (0.1% default)
        """
        try:
            # Get 52 weeks of data ending at check_date
            end_date = check_date
            start_date = end_date - timedelta(days=365)

            # Filter historical data
            historical_data = ticker_data[
                (ticker_data.index.date >= start_date) & 
                (ticker_data.index.date <= check_date)
            ]

            if len(historical_data) < 200:  # Need at least ~200 trading days for valid 52-week calc
                return False, None, None

            # Find the actual 52-week high
            week_52_high = historical_data['High'].max()
            week_52_high_date = historical_data['High'].idxmax().date()

            # Check if current high is at or above 52-week high (with tolerance)
            is_high = current_high >= (week_52_high * (1 - tolerance))

            return is_high, week_52_high, week_52_high_date

        except Exception as e:
            logger.error(f"Error in 52-week high calculation: {e}")
            return False, None, None

    def scan_weekly_highs(self, weeks_back=0, save_to_csv=False, save_to_excel=False):
        """
        Main function to scan for 52-week highs during the specified week

        Args:
            weeks_back: How many weeks back to check (0 = current week)
            save_to_csv: Save results to CSV file
            save_to_excel: Save results to Excel file
        """
        start_date, end_date = self.get_week_dates(weeks_back)

        logger.info(f"Scanning week: {start_date} to {end_date}")
        logger.info(f"Checking {len(self.nifty100_with_ns)} stocks for 52-week highs...")

        results = []
        success_count = 0
        error_count = 0

        # Process in batches to manage API calls
        batch_size = 10
        total_batches = (len(self.nifty100_with_ns) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(self.nifty100_with_ns))
            batch = self.nifty100_with_ns[start_idx:end_idx]

            logger.info(f"Processing batch {batch_num + 1}/{total_batches}")

            for ticker in batch:
                try:
                    stock = yf.Ticker(ticker)

                    # Get historical data (extra buffer for 52-week calculation)
                    hist_start = start_date - timedelta(days=400)
                    hist_data = stock.history(
                        start=hist_start,
                        end=end_date + timedelta(days=1),
                        interval="1d"
                    )

                    if len(hist_data) == 0:
                        logger.warning(f"No data for {ticker}")
                        error_count += 1
                        continue

                    # Check each trading day in the target week
                    week_data = hist_data[
                        (hist_data.index.date >= start_date) & 
                        (hist_data.index.date <= end_date)
                    ]

                    for date_idx, row in week_data.iterrows():
                        trade_date = date_idx.date()
                        day_high = row['High']
                        day_close = row['Close']
                        day_volume = row['Volume']

                        # Check if this day's high is a 52-week high
                        is_high, week_52_high, high_date = self.is_52_week_high(
                            hist_data, day_high, trade_date
                        )

                        if is_high:
                            # Get additional metrics
                            day_change = ((day_close - row['Open']) / row['Open']) * 100

                            results.append({
                                'Stock': ticker.replace('.NS', ''),
                                'Date': trade_date,
                                'High': round(day_high, 2),
                                'Close': round(day_close, 2),
                                'Volume': int(day_volume),
                                'Day_Change_%': round(day_change, 2),
                                '52W_High': round(week_52_high, 2),
                                'Previous_High_Date': high_date,
                                'Days_Since_Prev_High': (trade_date - high_date).days,
                                'New_High': 'Yes' if day_high > week_52_high else 'Matched'
                            })

                            logger.info(f" {ticker.replace('.NS', '')} hit 52W high on {trade_date}")
                            break  # Only record first 52W high in the week

                    success_count += 1

                except Exception as e:
                    logger.error(f"Error processing {ticker}: {str(e)[:100]}")
                    error_count += 1

        # Create results DataFrame
        if results:
            results_df = pd.DataFrame(results)
            results_df = results_df.sort_values(['Date', 'Stock']).reset_index(drop=True)
        else:
            results_df = pd.DataFrame()

        # Log summary
        logger.info(f"\nScan completed!")
        logger.info(f" Successfully processed: {success_count}")
        logger.info(f" Errors: {error_count}")
        logger.info(f" Found {len(results)} stocks with 52-week highs")

        # Save results if requested
        if save_to_csv and len(results_df) > 0:
            filename = f"nifty100_52w_highs_{start_date}_to_{end_date}.csv"
            results_df.to_csv(filename, index=False)
            logger.info(f" Results saved to {filename}")

        if save_to_excel and len(results_df) > 0:
            filename = f"nifty100_52w_highs_{start_date}_to_{end_date}.xlsx"
            results_df.to_excel(filename, index=False)
            logger.info(f" Results saved to {filename}")

        return results_df

    def get_near_52w_high_stocks(self, threshold_percent=5):
        """
        Get stocks that are within X% of their 52-week high

        Args:
            threshold_percent: Percentage threshold (default 5%)
        """
        logger.info(f"Finding stocks within {threshold_percent}% of 52-week high...")

        near_high_stocks = []

        for ticker in self.nifty100_with_ns[:20]:  # Limit for demo
            try:
                stock = yf.Ticker(ticker)
                hist_data = stock.history(period="1y")

                if len(hist_data) == 0:
                    continue

                current_price = hist_data['Close'].iloc[-1]
                week_52_high = hist_data['High'].max()
                week_52_low = hist_data['Low'].min()

                # Calculate distance from 52W high
                distance_percent = ((week_52_high - current_price) / week_52_high) * 100

                if distance_percent <= threshold_percent:
                    near_high_stocks.append({
                        'Stock': ticker.replace('.NS', ''),
                        'Current_Price': round(current_price, 2),
                        '52W_High': round(week_52_high, 2),
                        '52W_Low': round(week_52_low, 2),
                        'Distance_from_High_%': round(distance_percent, 2),
                        'Performance_52W_%': round(((current_price - week_52_low) / week_52_low) * 100, 2)
                    })

            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")

        return pd.DataFrame(near_high_stocks)

    def send_email_report(self, results_df, email_to, email_from, email_password, smtp_server="smtp.gmail.com", smtp_port=587):
        """
        Send email report of the results
        """
        try:
            msg = MIMEMultipart()
            msg['From'] = email_from
            msg['To'] = email_to
            msg['Subject'] = f"Nifty 100 Weekly 52-Week Highs Report - {datetime.now().strftime('%Y-%m-%d')}"

            if len(results_df) > 0:
                body = f"""
                Weekly 52-Week High Report
                =========================

                Found {len(results_df)} stocks that hit 52-week highs this week:

                {results_df.to_string(index=False)}

                Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                """
            else:
                body = """
                Weekly 52-Week High Report
                =========================

                No stocks hit 52-week highs during the scanned period.

                Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                """

            msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(email_from, email_password)

            text = msg.as_string()
            server.sendmail(email_from, email_to, text)
            server.quit()

            logger.info(f" Email sent to {email_to}")

        except Exception as e:
            logger.error(f"Error sending email: {e}")

def main():
    parser = argparse.ArgumentParser(description='Nifty 100 Weekly 52-Week High Detector')
    parser.add_argument('--weeks-back', type=int, default=0, help='Number of weeks back to check (default: current week)')
    parser.add_argument('--output', choices=['csv', 'excel', 'both'], help='Save output to file')
    parser.add_argument('--email', type=str, help='Email address to send results to')
    parser.add_argument('--near-high', action='store_true', help='Also show stocks near 52-week high')
    parser.add_argument('--threshold', type=float, default=5.0, help='Threshold percentage for near-high (default: 5%)')

    args = parser.parse_args()

    # Initialize detector
    detector = Nifty100WeeklyHighDetector()

    # Run the main scan
    save_csv = args.output in ['csv', 'both']
    save_excel = args.output in ['excel', 'both']

    results = detector.scan_weekly_highs(
        weeks_back=args.weeks_back,
        save_to_csv=save_csv,
        save_to_excel=save_excel
    )

    # Display results
    if len(results) > 0:
        print("\n" + "="*80)
        print(" STOCKS THAT HIT 52-WEEK HIGHS")
        print("="*80)
        print(results.to_string(index=False))
    else:
        print("\n No stocks hit 52-week highs during the scanned period.")

    # Show near-high stocks if requested
    if args.near_high:
        near_high_df = detector.get_near_52w_high_stocks(args.threshold)
        if len(near_high_df) > 0:
            print("\n" + "="*80)
            print(f" STOCKS NEAR 52-WEEK HIGH (within {args.threshold}%)")
            print("="*80)
            print(near_high_df.to_string(index=False))

    # Send email if requested
    if args.email:
        print(f"\n Email functionality requires SMTP configuration in the script.")
        print(f"   Recipient: {args.email}")

if __name__ == "__main__":
    main()
