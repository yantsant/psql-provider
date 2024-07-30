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

BLOCK_BATCH = 2000

BLOCK_DEPLOY = 20045981
VAUL_ADDRESS = '0xBEEF69Ac7870777598A04B2bd4771c71212E6aBc'
ETH_PRICE_ORACLE = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
TOPIC_DEPOSIT = "0xff195810018e2867a43eaac646e6b3fc71bc32d776175995704b6bc10d7fada8"  # Deposit (address, uint256[], uint256)
TOPIC_WITHDRAW = "0x9dd911706834ae347234c3bcfc7c9e4b74ba1f489af4c39c4481c4b527d4a63f" # WithdrawalsProcessed (address[], bool[] statuses)
TOPIC_ROLE_GRANTED = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d" # RoleGranted (bytes32, address, address)
TOPIC_ROLE_REVOKED = "0xf6391f5c32d9c69d2a47ea670b442974b53935d1edc7fd64eb21e047a839171b" # RoleRevoked (bytes32, address, address)

ROLE_ADMIN_DELEGATE = "0xc171260023d22a25a00a2789664c9334017843b831138c8ef03cc8897e5873d7"
ROLE_ADMIN = "0xf23ec0bb4210edd5cba85afd05127efcd2fc6a781bfed49188da1081670b22d8"
ROLE_OPERTOR = "0x46a52cf33029de9f84853745a87af28464c80bf0346df1b32e205fc73319f622"
ROLE_ADMIN_DEFAULT = "0x0000000000000000000000000000000000000000000000000000000000000000"
ROLE_ADMIN_DELEGATE_ID = 0
ROLE_ADMIN_ID = 1
ROLE_OPERTOR_ID = 2
ROLE_ADMIN_DEFAULT_ID = 3

LOG_RETRIVE_TIMEOUT_SECONDS = 12 # one ETH block
SLEEP_RETRY_TIMEOUT_SECONDS = 5

# SELECT to_timestamp("timestamp"), "totalSupply" FROM "table" ORDER BY "timestamp"
class VaultTVLData:
    def __init__(self, tiemstamp, block_number, total_supply, total_value, eth_price_usd) -> None:
        self.tiemstamp = int(tiemstamp)
        self.block_number = int(block_number)
        self.total_supply = float(total_supply)
        self.total_value = float(total_value)
        self.eth_price_usd = float(eth_price_usd)
        pass

class ProviderData:

    def __init__(self, vault_address, block_deploy, oracle_address, csv_storage_file_name) -> None:
        self.ROLE_ADMIN_DELEGATE = None
        self.ROLE_ADMIN = None
        self.ROLE_ADMIN_DEFAULT = None
        self.ROLE_OPERTOR = None
        self.http = None
        self.wss = None
        self.psql = None

        self.csv_file_name = csv_storage_file_name

        self.vault_address = vault_address
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

        self.block_deploy = block_deploy

        self.last_block_data = BLOCK_DEPLOY
        self.__get_vault_roles()
    
    """
        retrives role hashes 
    """
    def __get_vault_roles(self):
        self.ROLE_ADMIN = self.vault.functions.ADMIN_ROLE().call()
        self.ROLE_ADMIN_DELEGATE = self.vault.functions.ADMIN_DELEGATE_ROLE().call()
        self.ROLE_OPERTOR = self.vault.functions.OPERATOR().call()
        self.ROLE_ADMIN_DEFAULT = self.vault.functions.DEFAULT_ADMIN_ROLE().call()
        print(f"ROLE_ADMIN_DELEGATE: {ROLE_ADMIN_DELEGATE}\nROLE_ADMIN: {ROLE_ADMIN}\nROLE_OPERTOR: {ROLE_OPERTOR}\nROLE_ADMIN_DEFAULT: {ROLE_ADMIN_DEFAULT}")
        return
        self.psql.drop_table_data("rights")
        self.__write_right_hash_to_db(ROLE_ADMIN_ID, self.ROLE_ADMIN)
        self.__write_right_hash_to_db(ROLE_ADMIN_DELEGATE_ID, self.ROLE_ADMIN_DELEGATE)
        self.__write_right_hash_to_db(ROLE_OPERTOR_ID, self.ROLE_OPERTOR)
        self.__write_right_hash_to_db(ROLE_ADMIN_DEFAULT_ID, self.ROLE_ADMIN_DEFAULT)

    def __write_right_hash_to_db(self, id, hash):
        self.psql.write_data(
            """
            INSERT INTO public."rights" (id, hash)
            VALUES (%s, %s);
            """, [id, hash])
    
    def __write_addresses_right(self, block, timestamp, right_id, addresses):
        for address in addresses:
            self.psql.write_data(
                """
                INSERT INTO public."rights_history" (block, timestamp, rightid, address)
                VALUES (%s, %s, %s, %s);
                """, [block, timestamp, right_id, address])


    def run(self):
        self.__retrive_roles_data(BLOCK_DEPLOY-1)
        exit(1)
        remote_block = self.__get_last_block_from_remote()
        print("remote_block", remote_block)
        storage_block = self.__remote_sync_local(remote_block)

        start_block = max(storage_block, remote_block)

        last_block = self.http.eth.block_number

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
                    'topics': [[TOPIC_DEPOSIT, TOPIC_WITHDRAW]]
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
    def __retrive_roles_data(self, start_block):
        end_block = self.http.eth.block_number
        last_block = start_block
        print(f"start loop by new events from block {start_block}")
        for from_block in range(start_block, end_block, BLOCK_BATCH):
            to_block = min(from_block + BLOCK_BATCH - 1, end_block)
            try:
                logs_filter = self.http.eth.filter({
                    'address': [self.vault_address],
                    'fromBlock': from_block,
                    'toBlock': to_block,
                    'topics': [[TOPIC_ROLE_GRANTED, TOPIC_ROLE_REVOKED]]
                })
                logs = logs_filter.get_all_entries()
                for log in logs:
                    try:
                        if self.last_block_data < log.blockNumber and last_block != log.blockNumber:
                            admins = self.__get_address_by_role_at_block(ROLE_ADMIN, log.blockNumber)
                            admins_delegate = self.__get_address_by_role_at_block(ROLE_ADMIN_DELEGATE, log.blockNumber)
                            admins_default = self.__get_address_by_role_at_block(ROLE_ADMIN_DEFAULT, log.blockNumber)
                            operators = self.__get_address_by_role_at_block(ROLE_OPERTOR, log.blockNumber)
                            print(log.blockNumber, f"admins {admins}, admins_delegate {admins_delegate} operators {operators}, admins_default {admins_default}")
                            timestamp = self.__get_block_timestamp(log.blockNumber)
                            self.__write_addresses_right(log.blockNumber, timestamp, ROLE_ADMIN_ID, admins)
                            self.__write_addresses_right(log.blockNumber, timestamp, ROLE_ADMIN_DELEGATE_ID, admins_delegate)
                            self.__write_addresses_right(log.blockNumber, timestamp, ROLE_OPERTOR_ID, operators)
                            #self.__write_addresses_right(log.blockNumber, ROLE_ADMIN_ID, admins)
                            last_block = log.blockNumber

                    except Exception as e:
                        print(f"An error occurred at retriving on-chan data: {e}")
                        time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                        self.__http_connection_check()
                        continue
            except Exception as e:
                print(f"An error occurred at get_new_entries: {e}")
                time.sleep(SLEEP_RETRY_TIMEOUT_SECONDS)
                self.__wss_connection_check()
                continue

    def __get_address_by_role_at_block(self, role, block):
        addresses = []
        try:
            admin_counts = self.vault.functions.getRoleMemberCount(role).call(block_identifier=block)
            for idx in range(admin_counts):
                address = self.vault.functions.getRoleMember(role, idx).call(block_identifier=block)
                addresses.append(address)
        except Exception as e:
            print(f"An error occurred at retriving on-chan data: {e}")
        return addresses

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
                    'address': self.vault_address,
                    'topics': [[TOPIC_DEPOSIT, TOPIC_WITHDRAW]]
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
    
    """
        write one line into DB
    """
    def __write_row_db(self, data: VaultTVLData):
        self.psql.write_data(
            """
            INSERT INTO public."table" (vault, timestamp, "blockNumber", "totalSupply", "totalValue", "ethPrice")
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            [self.vault_address, data.tiemstamp, data.block_number, data.total_supply, data.total_value, data.eth_price_usd]
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
                            tvl_data = VaultTVLData(int(row[0]), row[1], row[2], row[3], row[4])
                            self.__write_row_db(tvl_data)
                    last_row = row
                    row_idx += 1
                if row_idx > 0:
                    return int(last_row[1])+1
                return BLOCK_DEPLOY
        else:
            print(f"The file {self.csv_file_name} does not exist -> block deploy is a start block")
            self.csv_writer.writerow(CSV_HEADER)
            return BLOCK_DEPLOY

    """
        gets contract instances
    """
    def __get_contracts(self):
        with open("./abi/vault.json") as f:
            vault_abi = json.load(f)
        self.vault = self.http.eth.contract(address=self.vault_address, abi=vault_abi)

        with open("./abi/oracle.json") as f:
            oracle_abi = json.load(f)
        self.oracle = self.http.eth.contract(address=self.oracle_address, abi=oracle_abi)

        self.configurator_address = self.vault.functions.configurator().call()

        with open("./abi/configurator.json") as f:
            configurator_abi = json.load(f)
        self.configurator = self.http.eth.contract(address=self.configurator_address, abi=configurator_abi)

        self.validator_address = self.configurator.functions.validator().call()

        print(f"""
        =========================== addresses ===========================
                 vault: {self.vault_address}
          configurator: {self.configurator_address}
             validator: {self.validator_address}
                oracle: {self.oracle_address}
        ================================================================""")
    
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

provider = ProviderData(VAUL_ADDRESS, BLOCK_DEPLOY, ETH_PRICE_ORACLE, "data/"+VAUL_ADDRESS+".csv")
provider.run()