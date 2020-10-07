import config
import psycopg2
from transaction import Transaction


class Database:
    connection = False
    cursor = False

    def __init__(self):
        self.connection = psycopg2.connect(host=config.database['host'], database=config.database['db'],
                                           user=config.database['user'], password=config.database['passwd'])
        self.cursor = self.connection.cursor()
        self.prepare_database()

    def update_transaction(self, transaction: Transaction):
        sql_instruction = "UPDATE {0} SET value = \'{1}\', gas = \'{2}\', gas_offered = \'{3}\'" \
                          ", tx_updated = true WHERE hash =\'{4}\';".format(
            config.database['table'], transaction.get_value(), transaction.get_gas_used(), transaction.get_gas_offered()
            , transaction.get_hash()
        )
        self.cursor.execute(sql_instruction)
        self.connection.commit()

    def prepare_database(self):
        sql_instruction = "SELECT EXISTS(SELECT column_name FROM information_schema.columns WHERE table_name=\'{0}\' " \
                          "and column_name='tx_updated');".format(config.database['table'])
        self.cursor.execute(sql_instruction)
        prepared = self.cursor.fetchone()

        if not prepared[0]:
            prepare_db_sql = "ALTER TABLE {0} ADD COLUMN tx_updated boolean DEFAULT false;".format(
                config.database['table'])
            self.cursor.execute(prepare_db_sql)
            self.connection.commit()
