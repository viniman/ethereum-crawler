import pandas as pd
import config
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:' + config.database['passwd'] +
                       '@localhost:5432/' + config.database['db'])


class Pandas:
    transaction_dataframe = False

    def __init__(self, by_csv: bool):
        if by_csv:
            self.transaction_dataframe = pd.read_csv(config.dataset_path, usecols=['hash'])
        else:
            self.transaction_dataframe = pd.read_sql(
                "SELECT hash from " + config.database['table'] + " WHERE joined_chain IS NOT NULL AND NOT tx_updated "
                                                                 "AND joined_pool < joined_chain LIMIT 100000 OFFSET " + config.instance_number,
                con=engine
            )
