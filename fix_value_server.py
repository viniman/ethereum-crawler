import pandas as pd
import os
import psycopg2


'''
    Atualiza dado de value das transações que tiveram o gas já atualizado com outro crawler
'''

database = {
    "host": "localhost",
    "user": "postgres",
    "passwd": "cp65482jf",
    "db": "ethscan",
    "table": "transactions4",
    "port": "5432"
}


def concatenate_datasets():
    
    all_files = get_files_path()
    df = pd.concat((pd.read_csv(file_name) for file_name in all_files))

    print(df.count())

    return df

def get_files_path(path='dataset_fix/'):
    all_files = os.path.join(path, "*.csv")
    return all_files


def list_of_csv(dir_path: str):
	"""get list of all csv files in given path

	Args:
		dir_path(str): absolute path to the source directory
	Returns:
	"""

	files = []
	#files.clear()
	#data.clear()

	try:
		for name in os.listdir(dir_path):
			if '.csv' in name:
				files.append(os.path.join(dir_path, name))
	except OSError:
		raise SystemExit(f'Path does not exist or you need to wrap the path inside quotes.')
	
	return files

def update_transactions(df):
    connection = psycopg2.connect(host=database['host'], database=database['db'],
                                           user=database['user'], password=database['passwd'])
    cursor = connection.cursor()

    for index, tx in df.iterrows():
        print(index, end=' ', flush=True)

        sql_instruction = "UPDATE {} SET value = \'{}\' WHERE hash =\'{}\';".format(
            database['table'], tx['value'], tx['hash']
        )

        cursor.execute(sql_instruction)
        connection.commit()




'''
pega o caminho para os arquivos csv
atualiza as transações de cada arquivo
'''
files_path = list_of_csv('fix_value')
print(files_path)
for index, file_path in zip(range(0,len(files_path)), files_path):
    print('-'*50)
    print ('CSV', index)
    df = pd.read_csv(file_path)
    update_transactions(df)



