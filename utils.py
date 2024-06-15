import os
import logging
import json
import subprocess
import traceback
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
import requests
import asyncpg

def set_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    script_name = Path(__file__).stem
    log_file = os.path.join(log_dir, f'{script_name}.log')

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

async def get_env():
    glassnode_api_key_name = 'GLASSNODE_API_KEY'
    db_url_name = 'TSDB_SERVICE_URL'
    telegram_token_name = 'TELEGRAM_GLASSNODE_SIGNAL_BULL_MARKET_BOT'
    telegram_chat_id_name = 'TELEGRAM_CHAT_ID'

    API_KEY = os.getenv(glassnode_api_key_name)
    DB_URL = os.getenv(db_url_name)
    TELEGRAM_TOKEN = os.getenv(telegram_token_name)
    TELEGRAM_CHAT_ID = os.getenv(telegram_chat_id_name)

    logging.info(f"Loading {glassnode_api_key_name}, {db_url_name}, {telegram_token_name}, and {telegram_chat_id_name} from environment variables")
    logging.info(f"{glassnode_api_key_name}: {API_KEY}")
    logging.info(f"{db_url_name}: {DB_URL}")
    logging.info(f"{telegram_token_name}: {TELEGRAM_TOKEN}")
    logging.info(f"{telegram_chat_id_name}: {TELEGRAM_CHAT_ID}")

    if API_KEY is None:
        logging.error(f"{glassnode_api_key_name} not found")
        log_and_telegram(logging.ERROR, f"{glassnode_api_key_name} not found", TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        sys.exit(1)
    if DB_URL is None:
        logging.error(f"{db_url_name} not found")
        log_and_telegram(logging.ERROR, f"{db_url_name} not found", TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        sys.exit(1)
    if TELEGRAM_TOKEN is None:
        logging.info(f"{telegram_token_name} Loaded")
    if TELEGRAM_CHAT_ID is None:
        logging.error(f"{telegram_chat_id_name} not found")
        sys.exit(1)

    logging.info(f"{glassnode_api_key_name} Loaded")
    logging.info(f"{db_url_name} Loaded")
    logging.info(f"{telegram_token_name} Loaded")
    logging.info(f"{telegram_chat_id_name} Loaded")
    
    # Extract host and port from DB_URL
    from urllib.parse import urlparse
    parsed_url = urlparse(DB_URL)
    host = parsed_url.hostname
    port = parsed_url.port

    # Test port connectivity
    test_port_connectivity(host, port, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    await test_db_connectivity(DB_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    return API_KEY, DB_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

async def get_metrics_from_file():
    metric_file_name = 'input.json'
    current_directory = os.getcwd()
    try:
        with open(metric_file_name, 'r') as f:
            config = json.load(f)
        logging.debug(f"Config file: {metric_file_name} loaded")
        validate_config(config)
        return config
    except FileNotFoundError:
        logging.error(f"Config file: {metric_file_name} not found in: {current_directory}")
        logging.error("Please create a config file named input.json")
        log_and_telegram(logging.ERROR, f"Config file: {metric_file_name} not found in: {current_directory}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON: {e}")
        log_and_telegram(logging.ERROR, "Failed to decode JSON", TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, error=str(e))
        sys.exit(1)

def validate_config(config):
    required_fields = ['symbols', 'endpoints']
    for field in required_fields:
        if field not in config:
            logging.error(f"Missing required field in config: {field}")
            log_and_telegram(logging.ERROR, f"Missing required field in config: {field}")
            sys.exit(1)
