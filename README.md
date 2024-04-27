# relayer-challenge

**Notes:**

- Please see the video demo below for a quick explanation/walkthrough of the code.

- block_crawler.py drops and (re)creates the 'blocks' and 'transactions' tables each time it is run.

Files:

- block_crawler.py (the database population script)  

- query.sql - a raw sql query to provide the info requested in the problem description

- results.txt - the block number and volume returned by the SQL query

- requirements.txt - project dependencies

To run:  

'python block_crawler.py' followed by the three positional arguments specified in the problem description (I use a postgres database) 

Ex.  python block_crawler.py https://quiknode.pro/key/ postgresql://postgres@localhost:5432/eth_db 18908800-18909050