import pandas as pd
from bs4 import BeautifulSoup
import time
from urllib.request import Request, urlopen

# read the dataset with the data collection by transactions, that contain users hash
df = pd.read_csv('../DataScience/ethereum/datasets/dataset_with_gas.csv', nrows=1)

url_default = 'https://etherscan.io/address/'
hdr = {'User-Agent': 'Mozilla/5.0'}
list_gases = []
list_values = []
list_gas_prices = []

s = time.time()

cont=0

for index, row in df.iterrows():
    cont +=1
    print (row['user_from'])
    url = url_default + row['user_from']
    req = Request(url,headers=hdr)
    page = urlopen(req)
    soup = BeautifulSoup(page, 'html.parser')
    
    values = soup.find_all('div', {'class': 'col-md-8'})

    print(type(values))

    for value in values[1:3]:
        print (value.get_text())

    
    #value = value.text.strip().split(' ', 1)[0].replace(',', '', 1)

    #gas_used = soup.find(id='ContentPlaceHolder1_spanGasUsedByTxn')
    #gas_used = gas_used.text.strip().split(' ', 1)[0].replace(',', '', 1)

    #list_gases.append(gas_used)
    #list_values.append(value)

    
    #print(cont)
    #print(index, gas_used, value)
    #print(index)
    #print(row)
    #print(row['hash'])
    #print(value)
    #print(gas_used)  

# atualiza csv
#hash_transactions['gas'] = list(map(int, list_gases))
#hash_transactions['value'] = list(map(float, list_values))

#hash_transactions.to_csv('../DataScience/ethereum/datasets/dataset_with_gas.csv', index=False)


print('Tempo', (time.time() - s))

