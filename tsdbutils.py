# mytsdbutils.py

from datetime import datetime, timezone
import logging
import json

###############
#
# Logging config
#
################

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('tsutils')

def log_message(level, message, **kwargs):
    log_entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'level': level,
        'message': message
    }
    log_entry.update(kwargs)
    logger.log(level, json.dumps(log_entry))



########################################
#
# ensure_table_setup
# To ensure table setup
#
########################################

async def ensure_table_setup(conn, table_name, column_definitions, primary_key=None, is_hypertable=False, hypertable_partition_column=None):
    query_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} (", column_definitions]
    if primary_key:
        query_parts.append(f", PRIMARY KEY ({primary_key})")
    query_parts.append(");")
    create_table_query = ''.join(query_parts)
    
    await conn.execute(create_table_query)
    
    if is_hypertable:
        create_hypertable_query = f"SELECT create_hypertable('{table_name}', '{hypertable_partition_column}');"
        try:
            await conn.execute(create_hypertable_query)
        except Exception as e:
            logger.warning(f"Hypertable creation skipped (it might already exist): {e}")



####################################################
#
# Function to insert data, with conflict resolution
# Supports DO NOTHING and UPDATE
# Uses conn.executemany for faster inserts
#
# await insert_data(conn, table_name, records, columns, conflict_action="DO NOTHING", conflict_columns=["column1", "column2"])
# await insert_data(conn, table_name, records, columns, conflict_action="DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2", conflict_columns=["key_column"])
#
#####################################################

async def insert_data(conn, table_name, records, columns, conflict_action=None, conflict_columns=None):
    # Create a string with placeholders for parameterized query
    placeholders = ', '.join(f"${n}" for n in range(1, len(columns) + 1))

    # Forming the ON CONFLICT clause
    conflict_clause = ""
    if conflict_columns and conflict_action:
        conflict_clause = f" ON CONFLICT ({', '.join(conflict_columns)}) {conflict_action}"

    # Create the INSERT INTO statement
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}){conflict_clause}"
    print(query)  # For debugging
    # Use executemany for inserting each record with the query
    await conn.executemany(query, records)



####################################################
#
# fetch_data
# Function to fetch data
# 
#####################################################

async def fetch_data(conn, table_name, columns="*", conditions="", params=()):
    # Build the SELECT statement
    query = f"SELECT {columns} FROM {table_name}"
    if conditions:
        query += f" WHERE {conditions}"
    
    # Execute the prepared statement with parameters
    return await conn.fetch(query, *params)
➜  site-packages ls
aiohttp                     docopt-0.6.2.dist-info              numpy.libs                   s3transfer                   tsutils-0.1.dist-info
aiohttp-3.9.1.dist-info     docopt.py                           pandas                       s3transfer-0.10.0.dist-info  tzdata
asyncpg                     __editable___tsutils_0_1_finder.py  pandas-2.1.2.dist-info       ta                           tzdata-2023.3.dist-info
asyncpg-0.29.0.dist-info    __editable__.tsutils-0.1.pth        pipreqs                      ta-0.11.0.dist-info          yarg
boto3                       jmespath                            pipreqs-0.4.13.dist-info     tests                        yarg-0.1.9.dist-info
boto3-1.34.49.dist-info     jmespath-1.0.1.dist-info            __pycache__                  tsdb_utils-0.1.dist-info
botocore                    numpy                               pytz                         tsdbutils-0.2.dist-info
botocore-1.34.49.dist-info  numpy-1.26.1.dist-info              pytz-2023.3.post1.dist-info  tsdbutils.py
➜  site-packages 
 *  History restored 

➜  site-packages 
➜  site-packages 
➜  site-packages 
➜  site-packages cd /home/dokhthdth/.local/lib/python3.12/site-packages
➜  site-packages cat tsdbutils
cat: tsdbutils: No such file or directory
➜  site-packages cat tsdbutils.py 
# mytsdbutils.py

from datetime import datetime, timezone
import logging
import json

###############
#
# Logging config
#
################

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('tsutils')

def log_message(level, message, **kwargs):
    log_entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'level': level,
        'message': message
    }
    log_entry.update(kwargs)
    logger.log(level, json.dumps(log_entry))



########################################
#
# ensure_table_setup
# To ensure table setup
#
########################################

async def ensure_table_setup(conn, table_name, column_definitions, primary_key=None, is_hypertable=False, hypertable_partition_column=None):
    query_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} (", column_definitions]
    if primary_key:
        query_parts.append(f", PRIMARY KEY ({primary_key})")
    query_parts.append(");")
    create_table_query = ''.join(query_parts)
    
    await conn.execute(create_table_query)
    
    if is_hypertable:
        create_hypertable_query = f"SELECT create_hypertable('{table_name}', '{hypertable_partition_column}');"
        try:
            await conn.execute(create_hypertable_query)
        except Exception as e:
            logger.warning(f"Hypertable creation skipped (it might already exist): {e}")



####################################################
#
# Function to insert data, with conflict resolution
# Supports DO NOTHING and UPDATE
# Uses conn.executemany for faster inserts
#
# await insert_data(conn, table_name, records, columns, conflict_action="DO NOTHING", conflict_columns=["column1", "column2"])
# await insert_data(conn, table_name, records, columns, conflict_action="DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2", conflict_columns=["key_column"])
#
#####################################################

async def insert_data(conn, table_name, records, columns, conflict_action=None, conflict_columns=None):
    # Create a string with placeholders for parameterized query
    placeholders = ', '.join(f"${n}" for n in range(1, len(columns) + 1))

    # Forming the ON CONFLICT clause
    conflict_clause = ""
    if conflict_columns and conflict_action:
        conflict_clause = f" ON CONFLICT ({', '.join(conflict_columns)}) {conflict_action}"

    # Create the INSERT INTO statement
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}){conflict_clause}"
    print(query)  # For debugging
    # Use executemany for inserting each record with the query
    await conn.executemany(query, records)



####################################################
#
# fetch_data
# Function to fetch data
# 
#####################################################

async def fetch_data(conn, table_name, columns="*", conditions="", params=()):
    # Build the SELECT statement
    query = f"SELECT {columns} FROM {table_name}"
    if conditions:
        query += f" WHERE {conditions}"
    
    # Execute the prepared statement with parameters
    return await conn.fetch(query, *params)
