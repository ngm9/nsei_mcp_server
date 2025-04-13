from typing import Any, Dict, Optional
import httpx
from mcp.server.fastmcp import FastMCP
import requests
import pandas as pd
import os
import tempfile
import zipfile
from datetime import datetime, timedelta
import logging
import sys
from logging.handlers import RotatingFileHandler


# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Configure logging
log_file = os.path.join(log_dir, 'nsei_mcp_server.log')

# Create formatters and handlers
file_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
console_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# File handler with rotation
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.DEBUG)


# Console handler

console_handler = logging.StreamHandler(sys.stderr)
console_handler.setFormatter(console_formatter)
console_handler.setLevel(logging.INFO)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# Create logger for this module
logger = logging.getLogger('nsei_mcp_server')
logger.info(f"Logging to file: {log_file}")

trades = pd.DataFrame() # global cache of trades

def _post_process_bhav_copy(df: pd.DataFrame) -> pd.DataFrame:
        """
        Post-process the Bhav Copy DataFrame to ensure it has the correct columns and data types.
        
        Args:
            df: pandas DataFrame containing the Bhav Copy data
        Returns:
            pandas DataFrame containing the post-processed Bhav Copy data
        """
        logger.info(f"Post-processing Bhav Copy for date: {df['TradDt'].iloc[0]}. Initial row count: {len(df)}")
        
        try:
            # Get unique values in SctySrs column
            unique_series = df['SctySrs'].unique()
            logger.debug(f"Found security series types: {unique_series}")
            
            # Filter for EQ series
            eq_rows = df[df['SctySrs'] == 'EQ']
            logger.info(f"Found {len(eq_rows)} rows with SctySrs = EQ")
            
            # Try case-insensitive match if no EQ rows found
            if len(eq_rows) == 0:
                logger.warning("No exact 'EQ' matches found, trying case-insensitive match")
                eq_rows = df[df['SctySrs'].str.upper() == 'EQ']
                logger.info(f"Found {len(eq_rows)} rows after case-insensitive match")
            
            if len(eq_rows) > 0:
                logger.debug("Sample of processed data:", extra={'data': eq_rows.head(3).to_dict()})
                return eq_rows
            else:
                logger.warning("No equity series data found, returning original dataframe")
                return df
                
        except Exception as e:
            logger.error(f"Error in post-processing: {str(e)}", exc_info=True)
            return df
       
def _download_bhav_copy(date: str):
    """
    Downloads the NSE Bhav Copy for a given date and returns it as a DataFrame.
    
    Args:
        date: Date string in format 'YYYY-MM-DD'
    
    Returns:
        pandas DataFrame containing the Bhav Copy data
    """
    logger.info(f"Downloading Bhav Copy for date: {date}")
    
    try:
        session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Handle date format conversion
        if '-' in date:
            try:
                parsed_date = datetime.strptime(date, "%Y-%m-%d")
                date = parsed_date.strftime("%Y%m%d")
                logger.debug(f"Converted date format: {date}")
            except ValueError as e:
                logger.error(f"Date parsing error: {str(e)}")
                return None
                
        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
        logger.debug(f"Downloading from URL: {url}")
        
        response = session.get(url, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Download failed with status code: {response.status_code}")
            return None
        
        # Process zip file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
            temp_zip.write(response.content)
            temp_zip_path = temp_zip.name
        
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                logger.error("No CSV file found in zip archive")
                os.unlink(temp_zip_path)
                return None
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
                temp_csv.write(zip_ref.read(csv_files[0]))
                temp_csv_path = temp_csv.name
        
        # Read and process data
        df = pd.read_csv(temp_csv_path)
        df = _post_process_bhav_copy(df)

        # Cleanup
        os.unlink(temp_zip_path)
        os.unlink(temp_csv_path)
        
        logger.info(f"Successfully downloaded and processed data. Row count: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}", exc_info=True)
        return None

def _get_data_for_date_range(date: str, ndays: int):
    """
    Get data for a specific date range from the cache.
    
    Args:
        date: date string in format 'YYYY-MM-DD'
        ndays: number of days to get data leading upto the given date
    """
    logger.info(f"Retrieving data for date: {date}, ndays: {ndays}")
    try:
        # Convert input date format from YYYY-MM-DD to YYYYMMDD for processing
        end_date = datetime.strptime(date, "%Y-%m-%d")
        start_date = end_date - timedelta(days=ndays-1)
        logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        

        # Generate list of dates needed
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)
        # Initialize an empty DataFrame to store all data
        all_data = pd.DataFrame()
        
        # Download data for each date in the range
        for date_obj in date_list:
            date_str = date_obj.strftime("%Y%m%d")
            logger.info(f"Downloading data for date: {date_str}")
            
            # Download bhav copy for this date
            day_data = _download_bhav_copy(date_str)
            
            # If data was successfully downloaded, add it to our collection
            if day_data is not None and not day_data.empty:
                all_data = pd.concat([all_data, day_data], ignore_index=True)
                logger.info(f"Added data for {date_str}, total rows now: {len(all_data)}")
            else:
                logger.warning(f"No data available for {date_str}")
        
        if all_data.empty:
            logger.error("No data was retrieved for the entire date range")
            return None
            
        logger.info(f"Successfully retrieved data for date range. Total rows: {len(all_data)}")
        logger.info(f"Sample of retrieved data:")
        logger.info(all_data.head())

        return all_data
        
    except Exception as e:
        logger.error(f"Error retrieving data for date range: {str(e)}", exc_info=True)
        return None

# Initialize FastMCP server and cache
#logger.info("Initializing NSEI MCP Server")
mcp = FastMCP("nsei")
logger.info("Server initialization complete")

@mcp.resource("nsei://trades/{date}")
async def trades(date: str):
    """
    Get trades for the given date
    
    Args:
        date: Date in format: "YYYY-MM-DD"
        
    Returns:
        a JSON specifying the trades.
    """
    logger.info(f"Fetching trades for date: {date}")
    try:
        # Convert date format from YYYY-MM-DD to YYYYMMDD for bhav copy
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%Y%m%d")
        
        # Download bhav copy for the specified date
        df = _download_bhav_copy(formatted_date)
        
        if df is None or df.empty:
            logger.warning(f"No data available for date: {date}")
            return {"error": "No data available for the specified date"}
        
        # Convert DataFrame to JSON
        result = df.to_dict(orient='records')
        logger.info(f"Successfully retrieved {len(result)} records for date: {date}")
        logger.info(f"Sample records: {df.head()}")

        return result
    except Exception as e:
        logger.error(f"Error retrieving trades for date {date}: {str(e)}", exc_info=True)
        return {"error": f"Failed to retrieve trades: {str(e)}"}
    
@mcp.tool()
async def get_top_movers(date: str, ndays: int = 1) -> Dict:
    """Get top movers for a period of ndays upto the given date.
    
    Args:
        date: Date in format 'YYYY-MM-DD'
        ndays: integer number of days leading up to date. default is 1
        
    Returns:
        Dictionary containing top gainers and losers with their details
    """
    logger.info(f"Starting get_top_movers for date: {date}, ndays: {ndays}")
    
    # Get data from cache for the date range
    df = _get_data_for_date_range(date, ndays)
    if df is None or len(df) == 0:
        logger.error("Error: No data available for the specified date range")
        return {"error": "No data available for the specified date range"}
    
    logger.info("Sample of retrieved data:")
    logger.info(df.head())
    
    # Process the data to find top movers
    # First get start and end prices for each symbol
    logger.info("Calculating start and end prices for each symbol...")
    
    # Check if we're dealing with single day data
    unique_dates = df['TradDt'].unique()
    logger.info(f"Number of unique dates in data: {len(unique_dates)}")
    
    if len(unique_dates) == 1:
        logger.info("Single day data detected - using open and close prices for the same day")
        # For single day, use open price as start and close price as end
        latest_data = df.set_index('TckrSymb')
        price_changes = pd.DataFrame({
            'start_price': latest_data['OpnPric'],
            'end_price': latest_data['ClsPric']
        })
    else:
        logger.info("Multiple days data detected - using first and last day closing prices")
        # For multiple days, use closing prices from first and last day
        start_prices = df.groupby('TckrSymb').first()[['OpnPric']]
        end_prices = df.groupby('TckrSymb').last()[['ClsPric']]
        price_changes = pd.DataFrame({
            'start_price': start_prices['OpnPric'],
            'end_price': end_prices['ClsPric']
        })

    # Calculate percentage changes
    price_changes['pct_change'] = ((price_changes['end_price'] - price_changes['start_price']) / 
                                price_changes['start_price'] * 100)
    
    logger.info("\nSample of calculated changes:")
    logger.info(price_changes.head())
    
    # Get additional details from the latest day's data
    latest_day_data = df[df['TradDt'].astype(str) == date]    
    if len(latest_day_data) == 0:
        logger.warning(f"Warning: No data found for exact date {date}. Available dates: {sorted(df['TradDt'].unique())}")
    
    latest_data = latest_day_data.set_index('TckrSymb')
    price_changes = price_changes.join(latest_data[['SctySrs', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'TtlTradgVol', 'TtlTrfVal']])
    logger.info(f"Final number of stocks after joining with latest day's data: {len(price_changes)}")
    
    # Sort by absolute percentage change to get top movers
    logger.info("Identifying top gainers and losers...")
    top_gainers = price_changes[price_changes['pct_change'] > 0].nlargest(10, 'pct_change')
    top_losers = price_changes[price_changes['pct_change'] < 0].nsmallest(10, 'pct_change')
    
    logger.info(f"Found {len(top_gainers)} gainers and {len(top_losers)} losers")
    logger.info("\nSample of top gainers:")
    logger.info(top_gainers.head())
    logger.info("\nSample of top losers:")
    logger.info(top_losers.head())
    
    # Format the results
    def format_movers(df):
        # Reset index to make sure we have unique indices before converting to dict
        df_reset = df.reset_index()
        # Use records orient instead of index to avoid unique index requirement
        return df_reset.round(2).to_dict('records')
    
    result = {
        "top_gainers": format_movers(top_gainers),
        "top_losers": format_movers(top_losers),
        "date_range": {
            "start": (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=ndays-1)).strftime("%Y-%m-%d"),
            "end": date
        }
    }
    
    logger.info("Successfully generated top movers report")
    return result

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
    #print(get_top_movers("2025-04-11", 1))
