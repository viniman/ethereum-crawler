# Atualiza ',' no gas e value, retirando as virgulas desses campos

# pegar block number e join do user_from e user_to para um arquivo csv

# criar um algoritmo para coletar dados de usuarios e blocos


import psycopg2
from sqlalchemy import create_engine
import pandas as pd

database = {
    "host": "localhost",
    "user": "postgres",
    "passwd": "cp65482jf",
    "db": "ethscan",
    "table": "transactions4",
    "port": "5432"
}


# coluna para atualizar
column = 'gas'

engine = create_engine('postgresql://postgres:' + database['passwd'] + '@localhost:5432/' + database['db'])


connection = psycopg2.connect(host=database['host'], database=database['db'], user=database['user'], password=database['passwd'])
cursor = connection.cursor()


df = pd.read_sql("SELECT hash, {} FROM {} WHERE {} like '%%,%%'".format(column, database['table'], column), con=engine)

for index, transaction in df.iterrows():

    print(transaction[column], end=' ')
    transaction[column] = transaction[column].replace(',', '')
    print(transaction[column])

    sql_instruction = "UPDATE {} SET {} = '{}' WHERE hash ='{}';".format(database['table'],
                column, transaction[column], transaction['hash'])
    
    cursor.execute(sql_instruction)
    connection.commit()