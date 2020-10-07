import config
import urllib.request
import json
from transaction import Transaction
from bs4 import BeautifulSoup


class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"

    def get_data_from_url(self, url):
        return self.open(url).read().decode()


class Etherscan:
    keys = []
    counter = 0
    opener = AppURLopener()

    # def __init__(self):
    #     self.keys = config.api_keys

    def get_transaction_information(self, tx_hash) -> Transaction:
        url = 'https://api.etherscan.io/api?module=proxy&action=eth_getTransactionByHash&txhash={0}&apikey={1}'.format(
            str(tx_hash), self.get_api_key())
        response = json.loads(self.opener.get_data_from_url(url))['result']
        transaction = Transaction(response)
        # transaction = self.get_transaction_gas_used(transaction)
        return transaction

    def get_api_key(self):
        key_round = self.counter
        key_round = key_round + 1
        self.counter = key_round % 2
        return str(config.api_keys[key_round])

    def get_transaction_gas_used(self, transaction: Transaction) -> Transaction:
        ethscan_url = 'https://etherscan.io/tx/' + transaction.tx_hash
        request = self.opener.open(ethscan_url)
        soup = BeautifulSoup(request, 'html.parser')
        gas_used = soup.find(id='ContentPlaceHolder1_spanGasUsedByTxn')
        gas_used = gas_used.text.strip().split(' ', 1)[0].replace(',', '')
        transaction.gas_used = gas_used
        return transaction

    def retrieve_transaction(self, tx_hash) -> Transaction:
        transaction = self.get_transaction_information(tx_hash)
        transaction = self.get_transaction_gas_used(transaction)
        return transaction
