from infura import Infura
import config
from database import Database


class Crawler:
    keys = []
    transactions = False
    api: Infura = False
    db = Database()

    def __init__(self, transactions):
        self.keys = config.api_keys
        self.transactions = transactions
        self.api = Infura()

    def start(self):
        print("Crawler started")
        for index, transaction in self.transactions.iterrows():
            try:
                retrieved_transaction = self.api.retrieve_transaction(transaction['hash'])
                retrieved_transaction.check_transaction()
                self.db.update_transaction(retrieved_transaction)
                print("Transaction " + transaction['hash'] + ' updated')
            except Exception as ex:
                print(ex)
                print("Problema ao atualizar transação " + transaction['hash'])
