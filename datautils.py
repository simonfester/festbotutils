import logging
import pandas as pd
import os
import traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

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

                if os.path.exists(file_path):
                    df = pd.read_parquet(file_path)
                    df.set_index('time', inplace=True)
                    df = df[~df.index.duplicated(keep='first')]
                    dataframes[metric_name] = df
                    logger.debug(f"Loaded new dataframe for {metric_name}")
                else:
                    logger.warning(f"File {file_path} does not exist. Fetching all data for {metric_name}.")
                    continue

    logger.debug("Finished loading dataframes")
    return dataframes

def load_dataframes_from_s3(bucket_name, config):
    logger.debug("Loading dataframes from S3")
    dataframes = {}

    for endpoint in config:
        logger.debug(f"Processing endpoint: {endpoint}")
        for asset in endpoint['assets']:
            for resolution in endpoint['resolutions']:
                metric_name = endpoint['path'].split('/')[-1]
                symbol = asset['symbol'].lower()
                file_key = f's3://{bucket_name}/data/raw/{symbol}_{resolution}_{metric_name}.parquet'

                try:
                    df = pd.read_parquet(file_key, storage_options={"anon": False})
                    df.set_index('time', inplace=True)
                    df = df[~df.index.duplicated(keep='first')]
                    dataframes[metric_name] = df
                    logger.debug(f"Loaded new dataframe for {metric_name}")
                except FileNotFoundError:
                    logger.warning(f"File {file_key} does not exist in S3 bucket {bucket_name}. Fetching all data.")
                    continue
                except (NoCredentialsError, PartialCredentialsError) as e:
                    logger.error(f"Error with AWS credentials: {e}")
                    raise
                except Exception as e:
                    logger.error(f"Error loading dataframe from S3: {e}")
                    logger.error(traceback.format_exc())
                    raise

    logger.debug("Finished loading dataframes")
    return dataframes

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

async def save_data_from_db_to_df(symbol, resolution, endpoint_name, db_pool, storage_type='local', bucket_name=None):
    table_name = f"{symbol.lower()}_{resolution}_{endpoint_name}"
    file_key = f's3://{bucket_name}/data/raw/{symbol.lower()}_{resolution}_{endpoint_name}.parquet' if storage_type == 's3' else f'data/raw/{symbol.lower()}_{resolution}_{endpoint_name}.parquet'
    since = None

    try:
        if storage_type == 's3' and bucket_name:
            df = pd.read_parquet(file_key, storage_options={"anon": False})
        else:
            if os.path.exists(file_key):
                df = pd.read_parquet(file_key)
        if not df.empty:
            last_timestamp = df['time'].max().to_pydatetime().replace(tzinfo=timezone.utc)
            buffer_time = get_buffer_time(resolution)
            since = calculate_next_timestamp(last_timestamp.timestamp(), resolution) + buffer_time
            logging.info(f"Last timestamp in {file_key} is {last_timestamp}. Fetching new data since {datetime.fromtimestamp(since, timezone.utc)} UTC")
    except FileNotFoundError:
        logging.warning(f"File {file_key} does not exist. Fetching all data.")
    except Exception as e:
        logging.error(f"Error loading dataframe: {e}")
        logging.error(traceback.format_exc())
        raise

    try:
        data = await fetch_data_from_db(db_pool, table_name, since)
        if data:
            save_data_to_dataframe(symbol, resolution, endpoint_name, data, df if 'df' in locals() else None, storage_type, bucket_name)
    except Exception as e:
        logging.error(f"Failed to fetch and save data for {symbol} {resolution} {endpoint_name}: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")

def save_data_to_dataframe(symbol, interval, endpoint_name, data, existing_df=None, storage_type='local', bucket_name=None):
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

    if storage_type == 's3' and bucket_name:
        file_key = f's3://{bucket_name}/data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet'
        df.to_parquet(file_key, index=False, storage_options={"anon": False})
        logger.info(f"Data saved to {file_key} with {len(df)} records")
    else:
        if not os.path.exists('data/raw'):
            os.makedirs('data/raw')
            logger.debug("Created data/raw directory")
        df.to_parquet(f'data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet')
        logger.info(f"Data saved to data/raw/{symbol.lower()}_{interval}_{endpoint_name}.parquet with {len(df)} records")
    logger.debug(f"DataFrame content: {df.head()}")

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

def load_dataframes_from_s3(bucket_name, prefix):
    fs = s3fs.S3FileSystem(anon=False)
    file_paths = fs.glob(f's3://{bucket_name}/{prefix}/*.parquet')
    dataframes = {}

    for file_path in file_paths:
        try:
            logger.info(f"Loading dataframe from {file_path}")
            df = pd.read_parquet(file_path, storage_options={'anon': False})
            dataframes[file_path.split('/')[-1].replace('.parquet', '')] = df
        except Exception as e:
            logger.error(f"Failed to load dataframe from {file_path}: {e}")
            raise

    return dataframes

def save_data_to_s3(bucket_name, key, df):
    fs = s3fs.S3FileSystem(anon=False)
    file_path = f's3://{bucket_name}/{key}'
    try:
        logger.info(f"Saving dataframe to {file_path}")
        with fs.open(file_path, 'wb') as f:
            df.to_parquet(f, index=False)
    except Exception as e:
        logger.error(f"Failed to save dataframe to {file_path}: {e}")
        raise
