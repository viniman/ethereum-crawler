import psycopg2
import urllib.request
import json
from random import shuffle


# ETHERSCAN KEYS
api_keys = ['YZVQYCEXE9FC7HEG22YE2S4X8CGG6QX86G',
            'QMRK95WAD8NBB41E7DD4EYHTDVNE5PBF6U',
            'CV6KPJ17NJP2IZ79QKX38EZVRRYUA3NTFZ',
            '1DND3R9DV71WM7FWXGMY3GPXH888CAQ2SC',
            '53KDADTQF4TKHXFJMQEKYUYXDEZ4F75Q8Y',
            'ABGJSQIDI2UMGJ89IIGEKA5N1WDQ98Z3CI']



# instance - 0 to n-1
instance_number = 9
# limit for each instance
limit = 30000


counter = 0

def get_api_key():
    global api_keys
    global counter
    key_round = counter
    key_round = key_round + 1
    counter = key_round % 6
    shuffle(api_keys)
    return str(api_keys[counter])


class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


opener = AppURLopener()


table = 'transactions4'

with psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf") as conn:

    cur = conn.cursor()

    transactions_hash = "select hash from {} where tx_updated and receipt_status is null LIMIT {} OFFSET {}".format(table, limit, instance_number*limit)
    cur.execute(transactions_hash)

    hashes = [ hash[0] for hash in cur.fetchall() ]

    #shuffle(hashes)



    for hash in hashes:
        
        try:
            url = 'https://api.etherscan.io/api?module=transaction&action=gettxreceiptstatus&txhash=0x{}&apikey={}'.format(hash, get_api_key())
            response = json.loads(opener.open(url).read().decode())['result']['status']
            if (response == '0' or response == '1'):
                sql = "UPDATE {} SET receipt_status = '{}' WHERE hash = '{}'".format(table, response, hash)
                print(sql, response)
                cur.execute(sql)
                conn.commit()
            else:
                print('Transaction {} not uptated.'.format(hash))
        
        except Exception as ex:
            print('Error:', ex)
            print('Transaction {} not uptated.'.format(hash))
            pass