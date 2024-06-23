import logging
import pandas as pd
import os
import traceback
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def load_dataframes_from_input_file(config: dict) -> dict:
    logger.debug("Loading dataframes from input file")
    dataframes = {}
    
    for endpoint in config:
        logger.debug(f"Processing endpoint: {endpoint}")
        for asset in endpoint['assets']:
            for resolution in endpoint['resolutions']:
                metric_name = endpoint['path'].split('/')[-1]
                symbol = asset['symbol'].lower()
                file_path = f'data/raw/{symbol}_{resolution}_{metric_name}.parquet'
                
                if metric_name not in dataframes:
                    df = pd.read_parquet(file_path)
                    df.set_index('time', inplace=True)
                    df = df[~df.index.duplicated(keep='first')]
                    dataframes[metric_name] = df
                    logger.debug(f"Loaded new dataframe for {metric_name}")
                else:
                    df = pd.read_parquet(file_path)
                    df.set_index('time', inplace=True)
                    df = df[~df.index.duplicated(keep='first')]
                    # Concatenate if multiple files need to be combined
                    dataframes[metric_name] = pd.concat([dataframes[metric_name], df])
                    logger.debug(f"Appended data to existing dataframe for {metric_name}")
    
    logger.debug("Finished loading dataframes")
    return dataframes

async def get_current_time():
    current_time = datetime.now(timezone.utc)
    current_time_unix = int(current_time.timestamp())
    current_time_human = current_time.strftime('%Y-%m-%d %H:%M:%S')
    logger.debug(f"Current time: {current_time_human} (unix: {current_time_unix})")
    return current_time_unix, current_time_human

def calculate_next_timestamp(last_unix_timestamp, interval):
    intervals = {
        "24h": 86400,
        "1h": 3600,
        "10m": 600
    }
    next_timestamp = int(last_unix_timestamp) + intervals.get(interval, 0)
    logger.debug(f"Calculated next timestamp: {next_timestamp} for interval {interval}")
    return next_timestamp

def get_buffer_time(interval):
    buffer_time = {
        "24h": 360,   # 
        "1h": 360,    # 6 minutes buffer to allow updaters to run for hourly data
        "10m": 360    # 6 minutes buffer to allow updaters to run for 10-minute data
    }
    buffer = buffer_time.get(interval, 0)
    logger.debug(f"Buffer time for interval {interval}: {buffer}")
    return buffer

async def fetch_data_from_db(pool, table_name, since=None):
    try:
        query = f"SELECT * FROM {table_name}"
        if since:
            query += f" WHERE time > to_timestamp({since})"
        query += " ORDER BY time"
        logging.debug(f"Querying database with: {query}")
        
        async with pool.acquire() as connection:
            logging.debug(f"Acquired connection from pool: {connection}")
            data = await connection.fetch(query)
            logging.info(f"Fetched {len(data)} records from {table_name}")
            logging.info(f"Since {datetime.fromtimestamp(since, timezone.utc)}" if since else "Fetching all data")
            logging.debug(f"Fetched data: {data}")
            return data
    except Exception as e:
        logging.error(f"Error fetching data from database: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return []

async def save_data_from_db_to_df(symbol, resolution, endpoint_name, db_pool):
    table_name = f"{symbol.lower()}_{resolution}_{endpoint_name}"
    parquet_file = f'data/raw/{symbol.lower()}_{resolution}_{endpoint_name}.parquet'

    since = None
    if os.path.exists(parquet_file):
        df = pd.read_parquet(parquet_file)
        if not df.empty:
            last_timestamp = df['time'].max().to_pydatetime().replace(tzinfo=timezone.utc)
            buffer_time = get_buffer_time(resolution)
            since = calculate_next_timestamp(last_timestamp.timestamp(), resolution) + buffer_time
            logging.info(f"Last timestamp in {parquet_file} is {last_timestamp}. Fetching new data since {datetime.fromtimestamp(since, timezone.utc)} UTC")

    try:
        data = await fetch_data_from_db(db_pool, table_name, since)
        if data:
            save_data_to_dataframe(symbol, resolution, endpoint_name, data, df if 'df' in locals() else None)
    except Exception as e:
        logging.error(f"Failed to fetch and save data for {symbol} {resolution} {endpoint_name}: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")

def save_data_to_dataframe(symbol, interval, endpoint_name, data, existing_df=None):
    logger.debug(f"Saving data to DataFrame for {symbol} {interval} {endpoint_name}")
    new_df = pd.DataFrame([dict(row) for row in data])
    if 'time' in new_df.columns:
        new_df['time'] = pd.to_datetime(new_df['time'], unit='s', utc=True)
    if existing_df is not None:
        df = pd.concat([existing_df, new_df]).drop_duplicates(subset='time').reset_index(drop=True)
        logger.debug(f"Concatenated new data with existing DataFrame for {symbol} {interval} {endpoint_name}")
    else:
        df = new_df
        logger.debug(f"Created new DataFrame for {symbol} {interval} {endpoint_name}")

    if not os.path.exists('data/raw'):
        os.makedirs('data/raw')
        logger.debug("Created data/raw directory")

    df.to_parquet(f'data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet')
    logger.info(f"Data saved to data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet with {len(df)} records")
    logger.debug(f"DataFrame content: {df.head()}")
