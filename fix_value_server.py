import pandas as pd
import os

#from sqlalchemy import create_engine
#engine = create_engine('sqlite://', echo=False)

import psycopg2


database = {
    "host": "localhost",
    "user": "postgres",
    "passwd": "cp65482jf",
    "db": "ethscan",
    "table": "transactions4",
    "port": "5432"
}


def concatenate_datasets():
    path = 'dataset_fix/'
    all_files = os.path.join(path, "*.csv")

    df = pd.concat((pd.read_csv(file_name) for file_name in all_files))

    print(df.count())

    return df


def update_transactions(df):
    connection = psycopg2.connect(host=database['host'], database=database['db'],
                                           user=database['user'], password=database['passwd'])
    cursor = connection.cursor()

    for index, tx in df.iterrows():
        print(index)

        sql_instruction = "UPDATE {0} SET value = \'{1}\', gas = \'{2}\', tx_updated = true WHERE hash =\'{4}\';".format(
            database['table'], tx['value'], tx['gas'], tx['hash']
        )

    cursor.execute(sql_instruction)
    connection.commit()


def main():
    
    df = concatenate_datasets()
    df.to_csv('dataset_entire_fix.csv', index=False)
    update_transactions(df)


    
if __name__ == '__main__':
   main() 
