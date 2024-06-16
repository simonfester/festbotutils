import os
import sys
import logging
import json
import subprocess
import traceback
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
import requests
import asyncpg
import aiohttp

def set_logging(log_filename=None):
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    if log_filename is None:
        script_name = Path(__file__).stem
        log_filename = f'{script_name}.log'

    log_file = os.path.join(log_dir, log_filename)

    # Create a rotating file handler
    file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # Configure the root logger
    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, console_handler])
    logging.info("Logging is set up")

def send_telegram_message(token, chat_id, message):
    """Send a message to a predefined Telegram chat."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()  # This will raise an exception for HTTP errors
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - {response.text}")
    except Exception as err:
        logging.error(f"Other error occurred: {err}")
    else:
        logging.info("Message sent to Telegram successfully.")

def log_and_telegram(level, message, token, chat_id, **kwargs):
    """Log message both locally and send to Telegram if level is ERROR or CRITICAL."""
    log_message = f"{message} | {json.dumps(kwargs)}"
    logging.log(level, log_message)
    if level >= logging.ERROR:
        send_telegram_message(token, chat_id, log_message)

def test_port_connectivity(host, port, token, chat_id):
    try:
        result = subprocess.run(
            ['nc', '-zv', host, str(port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            logging.info(f"Successfully connected to {host}:{port}")
        else:
            logging.error(f"Failed to connect to {host}:{port}")
            logging.error(result.stderr)
            log_and_telegram(logging.ERROR, f"Failed to connect to {host}:{port}", token, chat_id, stderr=result.stderr)
            sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred while testing port connectivity: {e}")
        logging.error(traceback.format_exc())
        log_and_telegram(logging.ERROR, "An error occurred while testing port connectivity", token, chat_id, error=str(e))
        sys.exit(1)

async def test_db_connectivity(db_url, token, chat_id):
    try:
        logging.info(f"Testing connectivity to the database: {db_url}")
        conn = await asyncpg.connect(db_url, timeout=10)
        await conn.close()
        logging.info("Successfully connected to the database.")
    except (asyncpg.exceptions.ConnectionDoesNotExistError, asyncpg.exceptions.InvalidAuthorizationSpecificationError) as e:
        logging.error(f"Failed to connect to the database: {e}")
        log_and_telegram(logging.ERROR, "Failed to connect to the database", token, chat_id, error=str(e))
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())
        log_and_telegram(logging.ERROR, "An unexpected error occurred", token, chat_id, error=str(e))
        sys.exit(1)

async def get_env(env_vars):
    env_values = {}
    missing_vars = []

    for var in env_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
        env_values[var] = value

    if missing_vars:
        logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    logging.info(f"Loaded environment variables: {', '.join(env_vars)}")
    return env_values

async def fetch_endpoints(api_key):
    url = 'https://api.glassnode.com/v2/metrics/endpoints'
    headers = {
        'X-Api-Key': api_key
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                logging.error(f"Failed to fetch endpoints: {response.status}")
                raise Exception(f"Failed to fetch endpoints: {response.status}")

async def get_metrics_from_file(api_key):
    metric_file_name = 'input.json'
    current_directory = os.getcwd()
    try:
        with open(metric_file_name, 'r') as f:
            config = json.load(f)
        logging.debug(f"Config file: {metric_file_name} loaded")
        
        endpoints = await fetch_endpoints(api_key)
        supported_metrics = {}
        
        for endpoint in endpoints:
            for asset in endpoint.get('assets', []):
                if asset['symbol'] not in supported_metrics:
                    supported_metrics[asset['symbol']] = []
                supported_metrics[asset['symbol']].append(endpoint['path'])
        
        validate_config(config, supported_metrics)
        return config
    except FileNotFoundError:
        logging.error(f"Config file: {metric_file_name} not found in: {current_directory}")
        logging.error("Please create a config file named input.json")
        raise FileNotFoundError(f"Config file: {metric_file_name} not found in: {current_directory}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON: {e}")
        raise ValueError(f"Failed to decode JSON: {e.msg} at line {e.lineno} column {e.colno}")

def validate_config(config, supported_metrics):
    required_fields = ['symbols', 'endpoints']
    for field in required_fields:
        if field not in config:
            logging.error(f"Missing required field in config: {field}")
            raise ValueError(f"Missing required field in config: {field}")
    
    unsupported_metrics = []
    for endpoint in config['endpoints']:
        for symbol in config['symbols']:
            if symbol not in supported_metrics or endpoint['url_path'] not in supported_metrics[symbol]:
                unsupported_metrics.append((symbol, endpoint['url_path']))
    
    if unsupported_metrics:
        error_messages = [f"Unsupported metric {metric} for symbol {symbol}" for symbol, metric in unsupported_metrics]
        for error_message in error_messages:
            logging.error(error_message)
        raise ValueError(f"Unsupported metrics found: {error_messages}")
