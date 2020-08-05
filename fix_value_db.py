import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from itertools import chain
import time
import sys


# suppress scientific notation
pd.options.display.float_format = '{:.10f}'.format #'{:,.10f}'.format


#count
count = 0


def get_value(hash):
    global count
    print(count, end=' ', flush=True)
    count+=1
    
    #if(count==1):
        #hash = hash + '9'

    #if(count==4):
        #hash = hash[0:len(hash)-1]

    url = url_view_tx + hash
    
    try:   
        req = Request(url,headers=hdr)
        page = urlopen(req)
        soup = BeautifulSoup(page, 'html.parser')
        value = soup.select_one('#ContentPlaceHolder1_spanValue > span')
        value = value.text.split(' ', 1)[0]

    except Exception as ex:
        print(ex)
        print('\nError:', hash)
        time.sleep(60)
        value = get_value(hash)
        pass

    return value



url_view_tx = 'https://etherscan.io/tx/0x'

# dataset com gas já atualizado, faltando o value:
read_path = '../DataScience/ethereum/datasets/dataset_value_gas_20200717.csv'
# caminho para os chunks serem salvos
path = '../DataScience/ethereum/datasets/dataset_value_gas_20200718_fix_'
hdr = {'User-Agent': 'Mozilla/5.0'}


# pass from command line arguments
# pass the number of the instance
instance = int(sys.argv[1])

nrows = 89774
dataset_size = 1615932
skiprows = chain(range(1,instance*nrows+1), range((instance+1)*nrows+1, dataset_size))


# divisao do dataset em 10 partes
chunks = 10
div = int(nrows/chunks)




# 89774 = 1615932/18 -> 0 .. 17
transactions = pd.read_csv(read_path, dtype={'value' : float}, usecols=['hash', 'value'], nrows=nrows, skiprows=skiprows)
#print(transactions)


time_geral = time.time()

for i in range(0,chunks):
    if(i < chunks-1):
        txs = transactions[i*div:(i+1)*div]
    else:
        txs = transactions[i*div:]
    #print(txs)

    start = time.time()
    txs['value'] = txs.apply(lambda row: get_value(row['hash']), axis=1)
    end = time.time()

    print('\n---------------------------------------------------')
    print('Time:', end - start)
    
    write_path = path + str(instance) + '_chunk_' + str(i) + '.csv'
    txs.to_csv(write_path, index=False)

    print('\n+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+')
    print('Time Geral:', time.time() - time_geral)



