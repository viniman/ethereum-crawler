import pandas as pd
from bs4 import BeautifulSoup
import time
from urllib.request import Request, urlopen


path = '../DataScience/ethereum/datasets/'

# read the dataset with the data collection by transactions, that contain users hash
df = pd.read_csv(path + 'dataset_fix_20200810.csv', usecols=['user_from', 'user_to'])


url_default = 'https://etherscan.io/address/'
hdr = {'User-Agent': 'Mozilla/5.0'}
list_gases = []
list_values = []
list_gas_prices = []

s = time.time()

count=0

user_from = df['user_from']
user_to = df['user_to']

print (user_from.shape)
print (user_to.shape)

accounts = user_from.append(user_to)

print (accounts.shape)

accounts = accounts.unique()

print (accounts.shape)


def get_info_account(account_hash):
    global url_default
    global urlopen
    global hdr
    global df

    url = url_default + account_hash

    try:
        req = Request(url,headers=hdr)
        page = urlopen(req)
        soup = BeautifulSoup(page, 'html.parser')


        # Scrap
        balance_ether = soup.select_one('#ContentPlaceHolder1_divSummary > div.row.mb-4 > div.col-md-6.mb-3.mb-md-0 > div > div.card-body > div:nth-child(1) > div.col-md-8')
        balance_value = soup.select_one('#ContentPlaceHolder1_divSummary > div.row.mb-4 > div.col-md-6.mb-3.mb-md-0 > div > div.card-body > div:nth-child(3) > div.col-md-8')
        total_transactions = soup.select_one('#transactions > div.d-md-flex.align-items-center.mb-3 > p > a')


        # Tratamento
        balance_ether = balance_ether.text.strip().split(' ', 1)[0].replace(',', '')
        if(balance_value.text[:4] == 'Less'):
            balance_value = balance_value.text.split(' ', 3)[2].strip()[1:].replace(',', '')
        else:
            balance_value = balance_value.text.strip()[1:].split(' ', 1)[0].replace(',', '')
        total_transactions = total_transactions.text.strip().replace(',', '')

        #print(balance_ether, balance_value, total_transactions)


        return [(account_hash, balance_ether, balance_value, total_transactions)]

    except Exception as ex:
        print('Error:', ex)
        print('Conta:', account_hash)
        print('Try again')
        df.to_csv(path + 'accounts_data.csv', index=False)
        get_info_account(account_hash)



df = pd.DataFrame(columns=['user_account', 'balance_ether', 'balance_value', 'total_transactions'])

for account_hash in accounts:
    count +=1
    time.sleep(0.5)
    print (count, end=' ', flush=True)
    user_data = get_info_account(account_hash)
    df = df.append(pd.DataFrame(user_data, columns=['user_account', 'balance_ether', 'balance_value', 'total_transactions']), ignore_index=True)
    #print(df, '\n')

    if(count % 100 == 0):
        df.to_csv(path + 'accounts_data.csv', index=False)


df.to_csv(path + 'accounts_data.csv', index=False) # , mode='a', header=False


print('\nTempo', (time.time() - s))

