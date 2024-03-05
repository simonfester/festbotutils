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
