from web3.auto.infura import w3
from transaction import Transaction
from etherscan import Etherscan


class Infura:
    etherscan: Etherscan = False

    def __init__(self):
        self.etherscan = Etherscan()
        print("Init Infura")

    def get_transaction_information(self, tx_hash) -> Transaction:
        # response = w3.eth.getTransaction(transaction_hash=tx_hash)
        # transaction = Transaction(response)
        # transaction = self.get_transaction_gas_used(transaction)
        # return transaction
        return self.etherscan.get_transaction_information(tx_hash=tx_hash)

    def get_transaction_gas_used(self, transaction: Transaction):
        response = w3.eth.waitForTransactionReceipt(transaction.tx_hash)
        transaction.gas_used = response.gasUsed
        return transaction

    def retrieve_transaction(self, tx_hash) -> Transaction:
        transaction = self.get_transaction_information(tx_hash)
        transaction = self.get_transaction_gas_used(transaction)
        return transaction

