import os
import sys
import logging
import json
import subprocess
import traceback
from pathlib import Path
import asyncpg
import aiohttp

logger = logging.getLogger(__name__)

async def combined_db_connectivity_test(db_url: str):
    """Test both port and database connectivity."""
    # Parse DB_URL to get host and port
    parsed_url = urlparse(db_url)
    host = parsed_url.hostname
    port = parsed_url.port

    # Test port connectivity
    try:
        result = subprocess.run(
            ['nc', '-zv', host, str(port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            logger.info(f"Successfully connected to {host}:{port}")
        else:
            logger.error(f"Failed to connect to {host}:{port}")
            logger.error(result.stderr)
            sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred while testing port connectivity: {e}")
        logger.error(traceback.format_exc())
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
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    logger.info(f"Loaded environment variables: {', '.join(env_vars)}")
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
                logger.error(f"Failed to fetch endpoints: {response.status}")
                raise Exception(f"Failed to fetch endpoints: {response.status}")

async def get_metrics_from_file(api_key):
    metric_file_name = 'input.json'
    current_directory = os.getcwd()
    try:
        with open(metric_file_name, 'r') as f:
            config = json.load(f)
        logger.debug(f"Config file: {metric_file_name} loaded")
        
        endpoints = await fetch_endpoints(api_key)
        supported_metrics = {}
        
        for endpoint in endpoints:
            for asset in endpoint.get('assets', []):
                if asset['symbol'] not in supported_metrics:
                    supported_metrics[asset['symbol']] = {}
                supported_metrics[asset['symbol']][endpoint['path']] = set(endpoint.get('resolutions', []))
        
        # Validate the config and return the updated config
        updated_config = validate_config(config, supported_metrics)
        return updated_config
    except FileNotFoundError:
        logger.error(f"Config file: {metric_file_name} not found in: {current_directory}")
        logger.error("Please create a config file named input.json")
        raise FileNotFoundError(f"Config file: {metric_file_name} not found in: {current_directory}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        raise ValueError(f"Failed to decode JSON: {e.msg} at line {e.lineno} column {e.colno}")

def validate_config(config, supported_metrics):
    required_fields = ['path', 'assets', 'resolutions']
    for endpoint in config:
        for field in required_fields:
            if field not in endpoint:
                logger.error(f"Missing required field in config: {field}")
                raise ValueError(f"Missing required field in config: {field}")

    updated_config = []
    unsupported_metrics = []

    for endpoint in config:
        path = endpoint['path']
        assets = endpoint['assets']
        resolutions = endpoint['resolutions']

        for asset in assets:
            symbol = asset['symbol']
            if symbol not in supported_metrics or path not in supported_metrics[symbol]:
                unsupported_metrics.append((symbol, path))
                continue

            for resolution in resolutions:
                if resolution not in supported_metrics[symbol][path]:
                    unsupported_metrics.append((symbol, path, resolution))
                    continue

            # Add supported metrics to the updated config
            updated_config.append({
                'path': path,
                'assets': [asset],
                'resolutions': resolutions
            })

    if unsupported_metrics:
        error_messages = [
            f"Unsupported metric {entry[1]} for symbol {entry[0]}" + (f" with resolution {entry[2]}" if len(entry) == 3 else "")
            for entry in unsupported_metrics
        ]
        for error_message in error_messages:
            logger.warning(error_message)

    return updated_config
