import urllib.request
import json
import psycopg2
import time


###############DEFINITIONS#############################

class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


class queryBuilder():
    def __init__(self, timestamp, transactions, block_num):
        self.timestamp = timestamp

        self.transactions = transactions
        for transaction in transactions:
            if transaction['from'] is not None and transaction['to'] is not None:
                cleanData(transaction)
                print(transaction)
                thisSQL = "UPDATE transactions3 tot SET joined_chain = '" + timestamp + "', block_number = '" + block_num + "', gas = '" + \
                          transaction['gas'] + "' WHERE joined_chain IS NULL AND hash = '" + transaction['hash'] + "';"
                print(thisSQL)
                checkIfSaved(thisSQL)
        print(block_num)


opener = AppURLopener()

conn = psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf")
cur = conn.cursor()

lastTimeStamp = '1999-10-21 00:00:00'

lastServedBlockSQL = 'SELECT max(block_number) from transactions3 WHERE joined_chain is not null;'
cur.execute(lastServedBlockSQL)
lastServedBlock = cur.fetchall()[0][0]

################FUNCTIONS##############################

def getLastBlockNumber():
    global lastServedBlock
    hexLastBlock = hex(lastServedBlock)
    lastServedBlock = lastServedBlock + 1
    return hexLastBlock


def getLastBlock():
    number = getLastBlockNumber()
    global lastTimeStamp
    int_block_number = cleanBlockNumber(number)
    url = 'https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag=' + number + '&boolean=true&apikey=QMRK95WAD8NBB41E7DD4EYHTDVNE5PBF6U'
    # try:
    response = json.loads(opener.open(url).read().decode())['result']
    timestamp = hexToTimestamp(response['timestamp'])
    if (lastTimeStamp != timestamp):
        lastTimeStamp = timestamp
        buildQuery = queryBuilder(timestamp, response['transactions'], int_block_number)
        # checkIfSaved((buildQuery.finalSQL))
    # except Exception as ex:
    #     print('getLastBlock expcetion')
    #     print(ex)


def hexToTimestamp(hour):
    hour = int(hour, 0)
    result = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(hour))
    return result


def checkIfSaved(sql):
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        conn.rollback()
        print("Problem at checkIfSaved")
        print(ex)


def cleanGasPrice(transaction):
    try:
        transaction['gasPrice'] = str(float(int(transaction['gasPrice'], 16)))
        return transaction
    except Exception as ex:
        print("cleanGasPrice")
        print(ex)


def cleanGas(transaction):
    try:
        transaction['gas'] = str(int(transaction['gas'], 16))
        return transaction
    except Exception as ex:
        print("cleanGas")
        print(ex)


def cleanValue(transaction):
    try:
        transaction['value'] = str(int(transaction['value'], 16))
        return transaction
    except Exception as ex:
        print("cleanValue")
        print(ex)


def cleanBlockNumber(data):
    try:
        return str(int(data, 16))
    except Exception as ex:
        print("cleanBlockNumber")
        print(ex)

def cleanHash(transaction):
    try :
        transaction['hash'] = transaction['hash'][2:]
        return transaction
    except Exception as ex:
        print("Clean Hash Exception")
        print(ex)


def cleanData(transaction):
    transaction = cleanGas(transaction)
    transaction = cleanGasPrice(transaction)
    transaction = cleanValue(transaction)
    transaction = cleanHash(transaction)

    return transaction


######################################################

while True:
    getLastBlock()
