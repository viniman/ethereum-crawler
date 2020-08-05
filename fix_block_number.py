import urllib.request

class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


# ETHERSCAN KEYS
api_keys = ['YZVQYCEXE9FC7HEG22YE2S4X8CGG6QX86G',
            'QMRK95WAD8NBB41E7DD4EYHTDVNE5PBF6U',
            'CV6KPJ17NJP2IZ79QKX38EZVRRYUA3NTFZ',
            '1DND3R9DV71WM7FWXGMY3GPXH888CAQ2SC',
            '53KDADTQF4TKHXFJMQEKYUYXDEZ4F75Q8Y',
            'ABGJSQIDI2UMGJ89IIGEKA5N1WDQ98Z3CI']

counter = 0


table = 'transactions4'


opener = AppURLopener()

def get_transaction_block_number(self, tx_hash) -> Transaction:
    global opener

    url = 'https://api.etherscan.io/api?module=proxy&action=eth_getTransactionByHash&txhash={}&apikey={}'.format(
        str(tx_hash), get_api_key())
    block_number = json.loads(opener.get_data_from_url(url))['result']['blockNumber']
    return block_number






with psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf") as conn:

    cur = conn.cursor()

    transactions_hash = "select hash from transactions4 where receipt_status is not null and block_number is null and tx_updated".format(table)
    cur.execute(transactions_hash)

    hashes = [ hash[0] for hash in cur.fetchall() ]

    shuffle(hashes)



    for hash in hashes:
        
        try:
            block_number = get_transaction_block_number(hash)
            
            sql = "UPDATE {} SET receipt_status = '{}' WHERE hash = '{}'".format(table, block_number, hash)
            print(sql, block_number)
            cur.execute(sql)
            conn.commit()
        
        except Exception as ex:
            print('Error:', ex)
            print('Transaction {} not uptated.'.format(hash))
            pass