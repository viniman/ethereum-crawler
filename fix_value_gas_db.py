import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import numpy as np
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

        gas_used = soup.select_one('#ContentPlaceHolder1_spanGasUsedByTxn')
        gas_used = gas_used.text.split(' ', 1)[0].replace(',', '')

        #print('value:', tx['value'], value)
        #print('gas:', tx['gas'], gas_used)

        tx['value'] = value
        tx['gas'] = gas_used

    except Exception:
        print('\nError:', tx['hash'], 'Value:',tx['value'])
        time.sleep(60)
        transactions.to_csv(write_path, index=False)
        update_transaction(index, tx)
        pass



# pass from command line arguments
# pass the number of the instance
instance = int(sys.argv[1])


url_view_tx = 'https://etherscan.io/tx/0x'
read_path = 'dataset_hash/dataset_hash_to_fix_' + str(instance) + '.csv'
write_path = 'dataset_fix/dataset_fix_value_gas_' + str(instance) + '.csv'
hdr = {'User-Agent': 'Mozilla/5.0'}



# 6840554 transacoes
# 7 pcs 
# 6840554/7 = 977222
# 977222/10 = 97722  -> 0 .. 9 (range)
# 10 crawlers por pc
# sobra 2 no ultimo
# 7*10 = 70 datasets: 0 .. 69


transactions = pd.read_csv(read_path)
print(transactions.count())

transactions['value'] = np.nan
transactions['gas'] = np.nan

start = time.time()

for index, tx in transactions.iterrows():
    update_transaction(index, tx)

    

    

    
end = time.time()
print(end - start)

transactions.to_csv(write_path, index=False)




