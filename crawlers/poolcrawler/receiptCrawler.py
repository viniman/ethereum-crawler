import psycopg2
import urllib.request
import json


class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


opener = AppURLopener()

conn = psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf")
cur = conn.cursor()

while True:
    transactionsHash = "SELECT hash FROM transactions3 WHERE joined_chain IS NOT NULL AND receipt_status IS NULL"
    cur.execute(transactionsHash)

    hashes = cur.fetchall()
    toCheck = []

    for hash in hashes:
        toCheck.append(hash[0])

    for hash in toCheck:
        url = 'https://api.etherscan.io/api?module=transaction&action=gettxreceiptstatus&txhash=0x' + hash + '&apikey=QMRK95WAD8NBB41E7DD4EYHTDVNE5PBF6U'
        response = json.loads(opener.open(url).read().decode())['result']['status']
        sql = "UPDATE transactions3 SET receipt_status = '" + response + "' WHERE hash = '" + hash + "'"
        print(sql)
        cur.execute(sql)
        conn.commit()
    conn.close()
