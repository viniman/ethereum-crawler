import psycopg2
import urllib.request
import json


class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


opener = AppURLopener()


table = 'transactions4'

with psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf") as conn:

    cur = conn.cursor()

    transactions_hash = "select hash from {} where tx_updated and receipt_status is null".format(table)
    cur.execute(transactions_hash)

    hashes = [ hash[0] for hash in cur.fetchall() ]


    for hash in hashes:
        url = 'https://api.etherscan.io/api?module=transaction&action=gettxreceiptstatus&txhash=0x{}&apikey=QMRK95WAD8NBB41E7DD4EYHTDVNE5PBF6U'.format(hash)
        response = json.loads(opener.open(url).read().decode())['result']['status']
        sql = "UPDATE {} SET receipt_status = '{}' WHERE hash = '{}'".format(table, response, hash)
        print(sql, response)
        cur.execute(sql)
        conn.commit()