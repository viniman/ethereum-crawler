import pandas as pd
from bs4 import BeautifulSoup
import time
from urllib.request import Request, urlopen

dfRead = pd.read_csv('../DataScience/ethereum/datasets/dataset_20190812.csv')

# pass to datetime type
dfRead.joined_pool = pd.to_datetime(dfRead.joined_pool)
dfRead.joined_chain = pd.to_datetime(dfRead.joined_chain)

# sort values by [joined_chain, joined_pool]
dfRead = dfRead.sort_values(by=['joined_chain', 'joined_pool'])
dfRead = dfRead.reset_index(drop=True)

# create time_pending column with data type
dfRead['time_pending_date'] = dfRead.joined_chain - dfRead.joined_pool

# time_pending column date type (datetime64) => int
dfRead['time_pending'] = pd.to_timedelta(dfRead.time_pending_date).dt.total_seconds().astype(int)

# Take off the negatives
dfRead = dfRead[dfRead.time_pending > 0]

# Inverting classes
dfRead.receipt_status = 1 - dfRead.receipt_status

# Sampling the dataset
dfRead = dfRead.sample(50000, random_state=2)

url_default = 'https://etherscan.io/tx/0x'
hdr = {'User-Agent': 'Mozilla/5.0'}
list_gases = []
list_values = []
list_gas_prices = []

s = time.time()

cont=0

for index, row in dfRead.iterrows():
    cont +=1
    url = url_default + row['hash']
    req = Request(url,headers=hdr)
    page = urlopen(req)
    soup = BeautifulSoup(page, 'html.parser')
    
    
    value = soup.find('span', {'class': 'u-label u-label--value u-label--secondary text-dark rounded mr-1'})
    value = value.text.strip().split(' ', 1)[0].replace(',', '', 1)

    gas_used = soup.find(id='ContentPlaceHolder1_spanGasUsedByTxn')
    gas_used = gas_used.text.strip().split(' ', 1)[0].replace(',', '', 1)

    list_gases.append(gas_used)
    list_values.append(value)

    
    print(cont)
    print(index, gas_used, value)
    #print(index)
    #print(row)
    #print(row['hash'])
    #print(value)
    #print(gas_used)  

# atualiza csv
dfRead['gas'] = list(map(int, list_gases))
dfRead['value'] = list(map(float, list_values))

dfRead.to_csv('../DataScience/ethereum/datasets/dataset_with_gas.csv', index=False)


print('Tempo', (time.time() - s))

