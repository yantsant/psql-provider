from web3 import Web3
import csv
import json
import os
import time
from psql import PostgreSQL, get_db_full_name
from dotenv import load_dotenv

load_dotenv()

CSV_HEADER = ['timestamp', 'block', 'totalSupply', 'totalValue', 'ethusd']

HTTP_RPC_URL = os.environ.get("HTTP_RPC_URL")
WSS_RPC_URL = os.environ.get("WSS_RPC_URL")

BLOCK_DEPLOY = 20045981
BLOCK_BATCH = 2000
VAUL_ADDRESS = '0xBEEF69Ac7870777598A04B2bd4771c71212E6aBc'
ETH_PRICE_ORACLE = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
DEPOSIT_TOPIC = "0xff195810018e2867a43eaac646e6b3fc71bc32d776175995704b6bc10d7fada8"  # Deposit (index_topic_1 address to, uint256[] amounts, uint256 lpAmount)
WITHDRAW_TOPIC = "0x9dd911706834ae347234c3bcfc7c9e4b74ba1f489af4c39c4481c4b527d4a63f" # WithdrawalsProcessed (address[] users, bool[] statuses)

LOG_RETRIVE_TIMEOUT_SECONDS = 12 # one ETH block
SLEEP_RETRY_TIMEOUT_SECONDS = 5

class VaultTVLData:
    def __init__(self, tiemstamp, block_number, total_supply, total_value, eth_price_usd) -> None:
        self.tiemstamp = int(tiemstamp)
        self.block_number = int(block_number)
        self.total_supply = float(total_supply)
        self.total_value = float(total_value)
        self.eth_price_usd = float(eth_price_usd)
        pass

class ProviderData:

    def __init__(self, contract_address, block_deploy, oracle_address, csv_storage_file_name, event_topics) -> None:
        self.http = None
        self.wss = None
        self.psql = None

        self.csv_file_name = csv_storage_file_name

        self.contract_address = contract_address
        self.oracle_address = oracle_address

        self.__http_connection_check()
        self.__wss_connection_check()
        self.__psql_connection_check()

        try:
            with open(csv_storage_file_name, mode='a', newline='') as csv_file:
                self.csv_writer = csv.writer(csv_file)
            with open(csv_storage_file_name, mode='r', newline='') as csv_file:
                self.csv_reader = csv.reader(csv_file)
        except Exception as e:
            print(f"Cannot open file {csv_storage_file_name}: {e}")
            return

        self.event_topics = event_topics
        self.block_deploy = block_deploy

        self.last_block_data = BLOCK_DEPLOY
        
    """
        gets contract instances
    """
    def __get_contracts(self):
        with open("./abi/vault.json") as f:
            vault_abi = json.load(f)
        self.vault = self.http.eth.contract(address=self.contract_address, abi=vault_abi)

        with open("./abi/oracle.json") as f:
            oracle_abi = json.load(f)
        self.oracle = self.http.eth.contract(address=self.oracle_address, abi=oracle_abi)
    
    """
        gets HTTP connection with retry
    """
    def __http_connection_check(self):
        if self.http is not None:
            if self.http.is_connected():
                return
            else:
                self.http = None
            
        while True:
            try:
                http = Web3(Web3.HTTPProvider(HTTP_RPC_URL))
            except Exception as e:
                print(f"An error occurred at HTTP connection: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                continue
            if http.is_connected():
                print("Connected to HTTP provider")
                self.http = http
                self.__get_contracts()
                return
            else:
                print("Connection to HTTP failed")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                continue

    """
        gets WSS connection with retry
    """
    def __wss_connection_check(self):
        if self.wss is not None:
            if self.wss.is_connected():
                return
            else:
                self.wss = None
            
        while True:
            try:
                wss = Web3(Web3.WebsocketProvider(WSS_RPC_URL))
            except Exception as e:
                print(f"An error occurred at WSS connection: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                continue
            if wss.is_connected():
                print("Connected to WSS provider")
                self.wss = wss
                return
            else:
                print("Connection to WSS failed")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                continue

    """
        gets PSQL connection with retry
    """
    def __psql_connection_check(self):
        if self.psql is not None:
            if self.psql.db_connection_check():
                return
            else:
                self.psql = None
            
        while True:
            try:
                psql = PostgreSQL()
            except Exception as e:
                print(f"Cannot connect to DB {get_db_full_name()}: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                continue
            if psql.db_connection_check():
                print(f"Connected to DB {get_db_full_name()}")
                self.psql = psql
                return
            else:
                print(f"Connection DB {get_db_full_name()} failed")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                continue

    def run(self):
        remote_block = self.__get_last_block_from_remote()
        print("remote_block", remote_block)
        storage_block = self.__remote_sync_local(remote_block)

        start_block = max(storage_block, remote_block)

        last_block = self.http.eth.block_number

        if remote_block<storage_block:
            self.__push_local_to_remote()
            pass

        self.__retrive_missed_data(start_block, last_block)

        self.__retrive_latest_data(start_block)

    """
        just get timestamp of the block
    """
    def __get_block_timestamp(self, block_number):
        while True:
            try:
                block = self.http.eth.get_block(block_number)
                return block['timestamp']
            except Exception as e:
                print(f"An error occurred at get_block {block_number}: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
    
    """
        collect missed on-chain data
    """
    def __retrive_missed_data(self, start_block, end_block):

        print(f"start grab missed data from blockchain, block range [{start_block}, {end_block}]")
        for from_block in range(start_block, end_block, BLOCK_BATCH):
            to_block = min(from_block + BLOCK_BATCH - 1, end_block)
            print(f"try to get logs for blocks [{from_block}, {to_block}]")
            try:
                logs_filter = self.http.eth.filter({
                    'fromBlock': from_block,
                    'toBlock': to_block,
                    'topics': [[DEPOSIT_TOPIC, WITHDRAW_TOPIC]]
                })
                logs = logs_filter.get_all_entries()
                for log in logs:
                    if self.last_block_data == log.blockNumber:
                        continue
                    tvl_data = self.__call_onchain_data(log.blockNumber)
                    self.__write_row_db(tvl_data)
                    self.last_block_data = log.blockNumber
                    print(tvl_data)
            except Exception as e:
                print(f"An error occurred at eth log filter: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                self.__http_connection_check()
                continue
            self.last_block_data = to_block + BLOCK_BATCH
        pass

    """
        collect on-chain data based on new evets
    """
    def __retrive_latest_data(self, start_block):
        print(f"start loop by new events from block {start_block}")
        while True:
            latest_block = self.http.eth.block_number
            try:
                logs = self.wss.eth.filter({
                    'fromBlock': self.last_block_data,
                    'toBlock': latest_block,
                    'address': self.contract_address,
                    'topics': [] #self.event_topics
                })
                logs = logs.get_new_entries()
            except Exception as e:
                print(f"An error occurred at get_new_entries: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                self.__wss_connection_check()
                continue

            logs = sorted(logs, key=lambda e: e['blockNumber'])

            for log in logs:
                try:
                    if self.last_block_data < log.blockNumber:
                        tvl_data = self.__call_onchain_data(log.blockNumber)
                        self.__write_row_db(tvl_data)
                        self.last_block_data = log.blockNumber
                        print(tvl_data)
                except Exception as e:
                    print(f"An error occurred at retriving on-chan data: {e}")
                    time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                    self.__http_connection_check()
                    continue

            self.last_block_data = latest_block
            print(f"block {self.last_block_data} obtained, wait for the next block")
            time.sleep(LOG_RETRIVE_TIMEOUT_SECONDS)
    
    """
        retrive on-chain data on the block_number
    """
    def __call_onchain_data(self, block_number):
        tvl_data = self.vault.functions.calculateStack().call(block_identifier=block_number)
        timestamp = self.__get_block_timestamp(block_number)
        eth_price_data = self.oracle.functions.latestRoundData().call(block_identifier=block_number)
        return VaultTVLData(tiemstamp=timestamp, block_number=block_number, total_supply=tvl_data[3], total_value=tvl_data[4], eth_price_usd=eth_price_data[1]/10**8)
    
    def __write_row_db(self, data: VaultTVLData):
        self.psql.write_data(
            """
            INSERT INTO public."table" (timestamp, "blockNumber", "totalSupply", "totalValue", "ethPrice")
            VALUES (%s, %s, %s, %s, %s);
            """,
            [data.tiemstamp, data.block_number, data.total_supply, data.total_value, data.eth_price_usd]
        )
    
    """
        retrive last written data by block_number from the remote DB 
    """ 
    def __get_last_block_from_remote(self):
        lines = self.psql.read_data("""
                                SELECT count("blockNumber") as lines FROM public."table"
                            """)
        if lines[0][0] == 0:
            return BLOCK_DEPLOY
        else:
            data = self.psql.read_data("""
                                    SELECT max("blockNumber") as last_block FROM public."table" WHERE "blockNumber" IS NOT NULL
                                """)
            return data[0][0]
    """
        pushes local data to remote DB
    """ 
    def __push_local_to_remote(slef):

        print("push data from local starage to remote DB")
        pass

    """
        retrive last written data by block_number from the storage 
    """ 
    def __remote_sync_local(self, remote_block):
        if os.path.exists(self.csv_file_name):
            print(f"The file {self.csv_file_name} already exists -> read last line")
            with open(self.csv_file_name, 'r', newline='') as csvfile:
                csvfile.flush()
                csvreader = csv.reader(csvfile)
                row_idx = 0
                #  CSV_HEADER = ['timestamp', 'blockNumber', 'totalSupply', 'totalValue', 'ethusd']
                for row in csvreader:
                    if row_idx>0:
                        if int(row[1]) > remote_block:
                            tvl_data = VaultTVLData(int(row[0])/1000, row[1], row[2], row[3], row[4])
                            self.__write_row_db(tvl_data)
                    last_row = row
                    row_idx += 1
                if row_idx > 0:
                    return int(last_row[1])+1
                return BLOCK_DEPLOY
        else:
            print(f"The file {self.csv_file} does not exist -> block deploy is a start block")
            self.csv_writer.writerow(CSV_HEADER)
            return BLOCK_DEPLOY

provider = ProviderData(VAUL_ADDRESS, BLOCK_DEPLOY, ETH_PRICE_ORACLE, "tvl.csv", [DEPOSIT_TOPIC, WITHDRAW_TOPIC])
provider.run()