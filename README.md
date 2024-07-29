# Python-PostgreSQL-Data-Provider

The main objective of this project is to create a simple Python service to fetch specific data from blockchain and push them inte PostgreSQL database.

### How it Works

During the runnig it store data locally in csv file and remotely in PsotrgeSQL database.
1. When it starts it is checking what timestamp is the last in remote DB and push if there is newer locally.
2. Then it fetch blockchain data from block on that we have data till the latest block at the calling the method.
3. After it connect to web socket to retrive the latest data by specific topics for the specific smart contract and push them into DB and local csv file.

### How to Run it

1. Set up environments
2. Set up abi files, contract address and topics
3. `pip install -r requirements.txt`
4. `python3 provider.py`
