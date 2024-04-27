import sys
import requests
import json
import psycopg2
from datetime import datetime, timezone

def main():
    num_args = len(sys.argv) - 1 # First entry is name of script

    if num_args < 3:
        print('Three inputs required - ETH endpoint, DB connection, and block range (start-end)')
        return

    endpoint, db_conn, block_range = sys.argv[1:]

    try:
        start_block, end_block = block_range.split('-')
        start_block = int(start_block)
        end_block = int(end_block)
    except ValueError:
        print('Block range must be of the form "start block #-end block #"')
        return
    
    headers = {
        'Content-Type': 'application/json'
    }

    conn = psycopg2.connect(db_conn)

    with conn:

        # (Re)create DB tables, FK constraint, and index
        with conn.cursor() as curs:
            curs.execute("""DROP TABLE IF EXISTS transactions;""")
            curs.execute("""DROP TABLE IF EXISTS blocks;""")
            curs.execute("""DROP TYPE IF EXISTS transaction;""")
            
            curs.execute("""CREATE TABLE blocks (
                id SERIAL PRIMARY KEY,
                hash TEXT UNIQUE NOT NULL,
                number TEXT UNIQUE NOT NULL,
                timestamp TIMESTAMP NOT NULL);
                """)
            
            # Use NUMERIC for type of 'value' to prevent signed 8-byte integer overflow
            curs.execute("""CREATE TABLE transactions (
                id SERIAL PRIMARY KEY,
                hash text UNIQUE NOT NULL,
                blockHash TEXT NOT NULL,
                blockNumber TEXT NOT NULL,
                value NUMERIC NOT NULL);
                """)
            
            curs.execute("""ALTER TABLE transactions
                            ADD CONSTRAINT fk_transactions_blocks FOREIGN KEY (blockNumber) REFERENCES blocks (number);
                        """)
            
            curs.execute("""CREATE INDEX blockNumber_index on transactions (blockNumber);""")
            
    # Persist to DB
    try:
        conn.commit()
    except:
        print('Error creating database schema - exiting')
        return
    
    # Retrieve blocks in specified range and populate DB
    for cur_block in range(start_block, end_block + 1):
        payload = json.dumps({
        "method": "eth_getBlockByNumber",
        "params": [hex(cur_block), True],
        "id": 1,
        "jsonrpc": "2.0"
        })

        response = requests.request("POST", endpoint, headers=headers, data=payload)

        if response.status_code != 200:
            print('ETH API call failed - please confirm the supplied arguments are correct')
            return
        
        block_data = response.json()['result']

        # Populate 'blocks' table
        with conn.cursor() as curs:
            utc_datetime = datetime.fromtimestamp(int(block_data["timestamp"], 16), tz=timezone.utc)
            curs.execute(f"""INSERT INTO blocks (hash, number, timestamp)
                            VALUES ('{block_data["hash"]}', '{block_data["number"]}', '{utc_datetime}');
                        """)
            
            # Populate 'transactions' table with transactions in cur_block
            for transaction in block_data["transactions"]:
                int_value = int(transaction["value"], 16)
                curs.execute(f"""INSERT INTO transactions (hash, blockHash, blockNumber, value)
                                VALUES ('{transaction["hash"]}', '{transaction["blockHash"]}', '{transaction["blockNumber"]}', '{int_value}');
                            """)

    # Persist and clean up
    try:   
        conn.commit()         
    except:
        print('Error writing to database')

    conn.close()

main()