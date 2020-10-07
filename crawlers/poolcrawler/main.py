import urllib.request
import json
import psycopg2
import time
from bs4 import BeautifulSoup
import re
import datetime


class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"

#class queryBuilder():
def queryBuilder(transactions):
    finalSQL = "INSERT INTO transactions3(joined_pool,user_from,user_to,hash,value,gas_offered,gas_price) VALUES"
    for transaction in transactions:
        if "hours" not in transaction['time']:
            transaction = cleanData(transaction)
            # print(json.dumps(transaction))
            sqlToDo = finalSQL + " ('" + transaction['time'] + "', '" + transaction['from'] + "', '" + transaction[
                'to'] + "','" + transaction['parenthash'] + "','" + str(int(float(transaction['value']))) + "','" + \
                      transaction['gas'] + "','" + transaction['gasprice'] + "')"
            insertIntoDB(sqlToDo)


###############DEFINITIONS#############################

qtdTransactions = 0

url = 'https://www.etherchain.org/txs/pending/data?draw=1&start=0&length=100'
#url_1 = 'https://www.etherchain.org/txs/pending/data?draw=1&start='
#url_2 = '&length=100'
opener = AppURLopener()

conn = psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="")#"cp65482jf")
cur = conn.cursor()

# ts = time.localtime()
# t2 = datetime.datetime.now()


################FUNCTIONS##############################

##Converts reponse time to current time
def convertTime(transaction):
    ts = time.localtime()
    t2 = datetime.datetime.now()

    if (transaction['time'] == 'a few seconds ago'):
        transaction['time'] = time.strftime("%Y-%m-%d %H:%M:%S", ts)
    elif transaction['time'] == 'a minute ago':
        ago = t2 - datetime.timedelta(minutes=int(1))
        transaction['time'] = time.strftime("%Y-%m-%d %H:%M:%S", ago.timetuple())
    else:
        try:
            if "hours" not in transaction['time']:
                m = re.search('([0-9]+)( minutes ago)', transaction['time'])
                ago = t2 - datetime.timedelta(minutes=int(m.group(1)))
                transaction['time'] = time.strftime("%Y-%m-%d %H:%M:%S", ago.timetuple())
    #         else:
    #             m = re.search('([0-9]+)( hours ago)', transaction['time'])
    #             ago = t2 - datetime.timedelta(hours=int(m.group(1)))
    #             transaction['time'] = time.strftime("%Y-%m-%d %H:%M:%S", ago.timetuple())
        except Exception as ex:
            print("Convert time exception")
            print(ex)
    return transaction


##Whole process of retrievieng the users of a transaction
def getUsers(transaction):
    getFrom = json.dumps(transaction['from'])
    soup = BeautifulSoup(getFrom, features='html.parser')
    for text in soup.findAll('a'):
        transaction['from'] = getFullName(text.get('href'))
    getTo = json.dumps(transaction['to'])
    soup = BeautifulSoup(getTo, features='html.parser')
    for text in soup.findAll('a'):
        transaction['to'] = getFullName(text.get('href'))
    return transaction


##Gets full name from the website given the URL retrieved from API response
def getFullName(url):
    try:
        m = re.search('(\/account\/)(\w*)', url)
    except Exception as ex:
        print("getFullName " + url)
        print(ex)
    return '0x' + m.group(2)


##Prepares the data recieved from API
def cleanData(transaction):
    # print(transaction)
    try:
        transaction = convertTime(transaction)
        transaction = getUsers(transaction)
        transaction = getTransactionHash(transaction)
        transaction = getTransactionValue(transaction)
        transaction = getGasPrice(transaction)
        return transaction
    except Exception as ex:
        print("Problem cleaning data")
        print(ex)

##Inserts transaction (pending) in DB

def insertIntoDB(resultSQL):
    try:
        global qtdTransactions
        words = resultSQL.split()
        if (words[-1] == 'VALUES'):
            print ("######################################entrou aqui porque entrou erradoo######################################")
            return
        cur.execute(resultSQL)
        conn.commit()
        qtdTransactions+=1
    except Exception as ex:
        conn.rollback()
        print(ex)


def getTransactionHash(transacion):
    try:
        soup = BeautifulSoup(json.dumps(transacion['parenthash']), features='html.parser')
        for text in soup.findAll('a'):
            transacion['parenthash'] = text.get('href')[4:]
        return transacion
    except Exception as ex:
        print("Problem getting transaction hash")
        print(ex)


def getTransactionValue(transaction):
    try:
        m = re.search('([0-9]+|\d+\.\d{1,2})?( ETH)', transaction['value'])
        transaction['value'] = str(float(m.group(1)) * 1000000000000000000)
        return transaction
    except Exception as ex:
        print("Problem converting value")
        print(ex)


def getGasPrice(transaction):
    try:
        m = re.search('([0-9]+|\d+\.\d{1,2})?( GWei)', transaction['gasprice'])
        if m.group(2) != ' GWei':
            print(transaction['gasprice'] + '\n')
            raise ValueError('This is not measured in GWei, unit used: ' + m.group(2))
        transaction['gasprice'] = str(float(m.group(1)) * 1000000000)
        return transaction
    except Exception as ex:
        print("getGasPrice Exception")
        print(transaction['gasprice'])
        print(ex)


def routine():
    print("INIT ROUTINE")
    try:
        #for i in range(2):
        #url = url_1 + str(0) + url_2
        global qtdTransactions
        global outFile
        print("INIT RESPONSE")
        response = json.loads(opener.open(url).read().decode())['data']
        queryBuilder(response)
        print("######QUANTIDADE DE TRANSACOES: ", qtdTransactions, "################")
        qtdTransactions = 0
        print("IR DORMIR")
        time.sleep(15)
    except Exception as ex:
        print(ex)
        #time.sleep(10)


#######################################################

while True:

    try:
        print("Started routine at " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        routine()
        print("Routine is over")
        #time.sleep(30)
    except Exception as ex:
        print(ex)

outFile.close()