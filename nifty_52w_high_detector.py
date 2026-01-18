import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
import warnings
warnings.filterwarnings('ignore')

# Nifty 100 stock symbols (with .NS suffix for NSE)
NIFTY_100_SYMBOLS = [
    'ABB.NS', 'ADANIENSOL.NS', 'ADANIENT.NS', 'ADANIGREEN.NS', 'ADANIPORTS.NS',
    'ADANIPOWER.NS', 'AMBUJACEM.NS', 'APOLLOHOSP.NS', 'ASIANPAINT.NS', 'AXISBANK.NS',
    'BAJAJ-AUTO.NS', 'BAJAJFINSV.NS', 'BAJAJHFL.NS', 'BAJAJHLDNG.NS', 'BAJFINANCE.NS',
    'BANKBARODA.NS', 'BEL.NS', 'BHARTIARTL.NS', 'BOSCHLTD.NS', 'BPCL.NS',
    'BRITANNIA.NS', 'CANBK.NS', 'CGPOWER.NS', 'CHOLAFIN.NS', 'CIPLA.NS',
    'COALINDIA.NS', 'DIVISLAB.NS', 'DLF.NS', 'DMART.NS', 'DRREDDY.NS',
    'EICHERMOT.NS', 'ENRIN.NS', 'ETERNAL.NS', 'GAIL.NS', 'GODREJCP.NS',
    'GRASIM.NS', 'HAL.NS', 'HAVELLS.NS', 'HCLTECH.NS', 'HDFCBANK.NS',
    'HDFCLIFE.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 'HINDZINC.NS', 'HYUNDAI.NS',
    'ICICIBANK.NS', 'ICICIGI.NS', 'INDHOTEL.NS', 'INDIGO.NS', 'INFY.NS',
    'IOC.NS', 'IRFC.NS', 'ITC.NS', 'JINDALSTEL.NS', 'JIOFIN.NS',
    'JSWENERGY.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LICI.NS', 'LODHA.NS',
    'LT.NS', 'LTIM.NS', 'M&M.NS', 'MARUTI.NS', 'MAXHEALTH.NS',
    'MAZDOCK.NS', 'MOTHERSON.NS', 'NAUKRI.NS', 'NESTLEIND.NS', 'NTPC.NS',
    'ONGC.NS', 'PFC.NS', 'PIDILITIND.NS', 'PNB.NS', 'POWERGRID.NS',
    'RECLTD.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS', 'SHREECEM.NS',
    'SHRIRAMFIN.NS', 'SIEMENS.NS', 'SOLARINDS.NS', 'SUNPHARMA.NS', 'TATACONSUM.NS',
    'TATAPOWER.NS', 'TATASTEEL.NS', 'TCS.NS', 'TECHM.NS', 'TITAN.NS',
    'TMPV.NS', 'TORNTPHARM.NS', 'TRENT.NS', 'TVSMOTOR.NS', 'ULTRACEMCO.NS',
    'UNITDSPR.NS', 'VBL.NS', 'VEDL.NS', 'WIPRO.NS', 'ZYDUSLIFE.NS'
]

def get_stock_data(symbol, period='1y'):
    """Fetch historical stock data"""
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period=period)
        return df
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def find_52_week_high_stocks(week_start=None, week_end=None):
    """
    Find stocks that hit 52-week high in a specific week
    
    Parameters:
    - week_start: Start date of the week (datetime object)
    - week_end: End date of the week (datetime object)
    """
    # Set timezone to IST (Indian Standard Time)
    ist = pytz.timezone('Asia/Kolkata')
    
    if week_start is None or week_end is None:
        # Default to current week
        today = datetime.now(ist)
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
    else:
        # Make sure provided dates are timezone-aware
        if week_start.tzinfo is None:
            week_start = ist.localize(week_start)
        if week_end.tzinfo is None:
            week_end = ist.localize(week_end)
    
    print(f"\n{'='*80}")
    print(f"NIFTY 100 - 52 WEEK HIGH SCANNER")
    print(f"{'='*80}")
    print(f"Analysis Period: {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")
    print(f"{'='*80}\n")
    
    stocks_at_52w_high = []
    
    for idx, symbol in enumerate(NIFTY_100_SYMBOLS, 1):
        print(f"Processing [{idx}/{len(NIFTY_100_SYMBOLS)}]: {symbol.replace('.NS', '')}", end='\r')
        
        df = get_stock_data(symbol, period='1y')
        
        if df is None or df.empty:
            continue
        
        # Get data for the specified week
        week_data = df[(df.index >= week_start) & (df.index <= week_end)]
        
        if week_data.empty:
            continue
        
        # Calculate 52-week high (excluding current week)
        data_before_week = df[df.index < week_start]
        
        if data_before_week.empty:
            continue
        
        week_52_high = data_before_week['High'].max()
        week_high = week_data['High'].max()
        current_price = df['Close'].iloc[-1]
        
        # Check if stock hit 52-week high during the week
        if week_high >= week_52_high * 0.999:  # 0.1% tolerance
            stock_name = symbol.replace('.NS', '')
            stocks_at_52w_high.append({
                'Symbol': stock_name,
                '52W High': round(week_52_high, 2),
                'Week High': round(week_high, 2),
                'Current Price': round(current_price, 2),
                'Date of High': week_data['High'].idxmax().strftime('%Y-%m-%d'),
                'Gain %': round(((week_high - week_52_high) / week_52_high) * 100, 2)
            })
    
    print("\n")
    
    # Create results DataFrame
    if stocks_at_52w_high:
        results_df = pd.DataFrame(stocks_at_52w_high)
        results_df = results_df.sort_values('Gain %', ascending=False)
        
        print(f"\n{'='*80}")
        print(f"STOCKS THAT HIT 52-WEEK HIGH THIS WEEK: {len(stocks_at_52w_high)} stocks found")
        print(f"{'='*80}\n")
        print(results_df.to_string(index=False))
        print(f"\n{'='*80}")
        
        # Summary statistics
        print(f"\nSUMMARY:")
        print(f"- Total stocks analyzed: {len(NIFTY_100_SYMBOLS)}")
        print(f"- Stocks at 52-week high: {len(stocks_at_52w_high)}")
        print(f"- Percentage: {round((len(stocks_at_52w_high)/len(NIFTY_100_SYMBOLS))*100, 2)}%")
        
        return results_df
    else:
        print("No stocks hit 52-week high during this week.")
        return pd.DataFrame()

def scan_multiple_weeks(num_weeks=4):
    """Scan multiple weeks for 52-week highs"""
    results = {}
    ist = pytz.timezone('Asia/Kolkata')
    
    for i in range(num_weeks):
        week_end = datetime.now(ist) - timedelta(weeks=i)
        week_start = week_end - timedelta(days=week_end.weekday())
        week_end = week_start + timedelta(days=6)
        
        week_label = f"Week {week_start.strftime('%Y-%m-%d')}"
        results[week_label] = find_52_week_high_stocks(week_start, week_end)
    
    return results

# Main execution
if __name__ == "__main__":
    # Scan current week
    current_week_results = find_52_week_high_stocks()
    
    # Optional: Scan last 4 weeks
    # print("\n\nScanning last 4 weeks...")
    # multi_week_results = scan_multiple_weeks(4)
