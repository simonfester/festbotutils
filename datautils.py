import logging
import asyncio
import asyncpg
import pandas as pd
import traceback
import sys
import os
from datetime import datetime, timezone
from asyncpg import create_pool
from urllib.parse import urlparse
from tsdbutils import ensure_table_setup, insert_data
from utils import set_logging, send_telegram_message, log_and_telegram, test_port_connectivity, test_db_connectivity, get_env, get_metrics_from_file

def load_dataframes_from_input_file(config: dict) -> dict:
    dataframes = {}
    
    for endpoint in config:
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
                else:
                    df = pd.read_parquet(file_path)
                    df.set_index('time', inplace=True)
                    df = df[~df.index.duplicated(keep='first')]
                    # Concatenate if multiple files need to be combined
                    dataframes[metric_name] = pd.concat([dataframes[metric_name], df])
    
    return dataframes



async def get_current_time():
    current_time = datetime.now(timezone.utc)
    current_time_unix = int(current_time.timestamp())
    current_time_human = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return current_time_unix, current_time_human

def calculate_next_timestamp(last_unix_timestamp, interval):
    intervals = {
        "24h": 86400,
        "1h": 3600,
        "10m": 600
    }
    return int(last_unix_timestamp) + intervals.get(interval, 0)

def get_buffer_time(interval):
    buffer_time = {
        "24h": 3600,  # 1 hour buffer for daily data, because daily data has a 24-hour lag
        "1h": 360,    # 6 minutes buffer to allow updaters to run for hourly data
        "10m": 360    # 6 minutes buffer to allow updaters to run for 10-minute data
    }
    return buffer_time.get(interval, 0)

async def fetch_data_from_db(pool, table_name, since=None):
    query = f"SELECT * FROM {table_name}"
    if since:
        query += f" WHERE time > to_timestamp({since})"
    query += " ORDER BY time"
    logging.debug(f"Querying database with: {query}")
    async with pool.acquire() as connection:
        data = await connection.fetch(query)
        logging.info(f"Fetched {len(data)} records from {table_name} since {since}")
        return data

async def save_data_from_db_to_df(symbol, interval, endpoint_name, db_pool, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    table_name = f"{symbol.lower()}_{interval}_{endpoint_name}"
    parquet_file = f'data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet'

    since = None
    if os.path.exists(parquet_file):
        df = pd.read_parquet(parquet_file)
        if not df.empty:
            last_timestamp = df['time'].max().to_pydatetime().replace(tzinfo=timezone.utc)
            buffer_time = get_buffer_time(interval)
            since = calculate_next_timestamp(last_timestamp.timestamp(), interval) + buffer_time
            logging.info(f"Last timestamp in {parquet_file} is {last_timestamp}. Fetching new data since {datetime.fromtimestamp(since, timezone.utc)} UTC")

    try:
        data = await fetch_data_from_db(db_pool, table_name, since)
        if data:
            save_data_to_dataframe(symbol, interval, endpoint_name, data, df if 'df' in locals() else None)
    except Exception as e:
        logging.error(f"Failed to fetch and save data for {symbol} {interval} {endpoint_name}: {e}")
        log_and_telegram(logging.ERROR, f"Failed to fetch and save data for {symbol} {interval} {endpoint_name}", TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, error=str(e))

def save_data_to_dataframe(symbol, interval, endpoint_name, data, existing_df=None):
    new_df = pd.DataFrame([dict(row) for row in data])
    if 'time' in new_df.columns:
        new_df['time'] = pd.to_datetime(new_df['time'], unit='s', utc=True)
    if existing_df is not None:
        df = pd.concat([existing_df, new_df]).drop_duplicates(subset='time').reset_index(drop=True)
    else:
        df = new_df

    if not os.path.exists('data/raw'):
        os.makedirs('data/raw')
        logging.debug("Created data/raw directory")

    df.to_parquet(f'data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet')
    logging.info(f"Data saved to data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet with {len(df)} records")

