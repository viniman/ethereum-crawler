import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from itertools import chain

import time
import sys


def update_transaction(index, tx):
    print(index, end=' ', flush=True)

    url = url_view_tx + tx['hash']

    #print(tx['hash'], tx['value'], end=' ')

    try:

        
        req = Request(url,headers=hdr)
        page = urlopen(req)
        soup = BeautifulSoup(page, 'html.parser')

        value = soup.select_one('#ContentPlaceHolder1_spanValue > span')
        value = value.text.split(' ', 1)[0]

        tx['value'] = value
        #print(tx['value'])
        #print('Transaction atualizada:', tx['hash'])

    except Exception:
        print('\nError:', tx['hash'], 'Value:',tx['value'])
        time.sleep(60)
        transactions.to_csv(write_path, index=False)
        update_transaction(index, tx)
        pass




url_view_tx = 'https://etherscan.io/tx/0x'
read_path = '../DataScience/ethereum/datasets/dataset_value_gas_20200717.csv'
write_path = '../DataScience/ethereum/datasets/dataset_value_gas_20200717_fix_'
hdr = {'User-Agent': 'Mozilla/5.0'}


# pass from command line arguments
# pass the number of the instance
instance = int(sys.argv[1])

write_path = write_path + str(instance) + '.csv'
print(write_path)


nrows = 89774
dataset_size = 1615932
skiprows = chain(range(1,instance*nrows+1), range((instance+1)*nrows+1, dataset_size))

# 89774 = 1615932/18 -> 0 .. 17
transactions = pd.read_csv(read_path, dtype={'value' : float}, usecols=['hash', 'value'], nrows=nrows, skiprows=skiprows)
print(transactions.count())

start = time.time()

for index, tx in transactions.iterrows():
    update_transaction(index, tx)
    
    
    
end = time.time()
print('\nTime:', end - start)

transactions.to_csv(write_path, index=False)




