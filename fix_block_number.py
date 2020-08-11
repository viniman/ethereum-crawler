import psycopg2
import urllib.request
import json
from random import shuffle

class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"

    def get_data_from_url(self, url):
        return self.open(url).read().decode()


# ETHERSCAN KEYS
api_keys = ['YZVQYCEXE9FC7HEG22YE2S4X8CGG6QX86G',
            'QMRK95WAD8NBB41E7DD4EYHTDVNE5PBF6U',
            'CV6KPJ17NJP2IZ79QKX38EZVRRYUA3NTFZ',
            '1DND3R9DV71WM7FWXGMY3GPXH888CAQ2SC',
            '53KDADTQF4TKHXFJMQEKYUYXDEZ4F75Q8Y',
            'ABGJSQIDI2UMGJ89IIGEKA5N1WDQ98Z3CI']


counter = 0

def get_api_key():
    global api_keys
    global counter
    key_round = counter
    key_round = key_round + 1
    counter = key_round % 6
    shuffle(api_keys)
    return str(api_keys[counter])



table = 'transactions4'


opener = AppURLopener()

def get_transaction_block_number(tx_hash):
    global opener

    url = 'https://api.etherscan.io/api?module=proxy&action=eth_getTransactionByHash&txhash={}&apikey={}'.format(
        str(tx_hash), get_api_key())
    block_number = json.loads(opener.get_data_from_url(url))['result']['blockNumber']
    return block_number






with psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf") as conn:

    cur = conn.cursor()

    count = "select count(hash) from transactions4 where receipt_status is not null and block_number is null and tx_updated".format(table)
    cur.execute(count)
    print(cur.fetchall())

    transactions_hash = "select hash from transactions4 where receipt_status is not null and block_number is null and tx_updated".format(table)
    cur.execute(transactions_hash)

    hashes = [ hash[0] for hash in cur.fetchall() ]

    shuffle(hashes)



    for hash in hashes:
        
        try:
            block_number = get_transaction_block_number(hash)

            sql = "UPDATE {} SET block_number = '{}' WHERE hash = '{}'".format(table, int(block_number, 16), hash)
            print(sql, block_number)
            cur.execute(sql)
            conn.commit()
        
        except Exception as ex:
            print('Error:', ex)
            print('Transaction {} not uptated.'.format(hash))
            pass