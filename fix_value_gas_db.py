import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import numpy as np
import time
import sys


# suppress scientific notation
pd.options.display.float_format = '{:.10f}'.format #'{:,.10f}'.format


#count
count = 0



def get_value_gas(hash):
    global count
    print(count, end=' ', flush=True)
    count+=1

    url = url_view_tx + hash
    
    try:   
        req = Request(url,headers=hdr)
        page = urlopen(req)
        soup = BeautifulSoup(page, 'html.parser')

        value = soup.select_one('#ContentPlaceHolder1_spanValue > span')
        value = value.text.split(' ', 1)[0]

        gas_used = soup.select_one('#ContentPlaceHolder1_spanGasUsedByTxn')
        gas_used = gas_used.text.split(' ', 1)[0].replace(',', '')

    except Exception as ex:
        print(ex)
        print('\nError:', hash)
        time.sleep(60)
        get_value_gas(hash)
        pass

    return value, gas_used



# pass from command line arguments
# pass the number of the instance
instance = int(sys.argv[1])


url_view_tx = 'https://etherscan.io/tx/0x'
read_path = 'dataset_hash/dataset_hash_to_fix_' + str(instance) + '.csv'
path = 'dataset_fix/dataset_fix_value_gas_'
hdr = {'User-Agent': 'Mozilla/5.0'}



# 6840554 transacoes
# 7 pcs 
# 6840554/7 = 977222
# 977222/10 = 97722  -> 0 .. 9 (range)
# 10 crawlers por pc
# sobra 2 no ultimo
# 7*10 = 70 datasets: 0 .. 69


transactions = pd.read_csv(read_path, nrows=50)


# divisao do dataset em 10 partes
chunks = 10
div = int(len(transactions)/chunks)

print ('tamanho', len(transactions))




transactions['value'] = np.nan
transactions['gas'] = np.nan



time_geral = time.time()


for i in range(0,chunks):
    if(i < chunks-1):
        txs = transactions[i*div:(i+1)*div]
    else:
        txs = transactions[i*div:]
    #print(txs)

    start = time.time()
    txs['value'], txs['gas'] = zip(*txs.apply(lambda row: get_value_gas(row['hash']), axis=1))
    end = time.time()

    print('\n---------------------------------------------------')
    print('Time:', end - start)
    
    write_path = path + str(instance) + '_chunk_' + str(i) + '.csv'
    txs.to_csv(write_path, index=False)

    print('\n+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+')
    print('Time Geral:', time.time() - time_geral)
    




