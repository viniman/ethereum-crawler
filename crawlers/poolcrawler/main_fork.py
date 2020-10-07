import urllib.request
import json
import psycopg2
import time
from bs4 import BeautifulSoup
import re
import datetime


class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


class queryBuilder():
    def __init__(self, timestamp, transactions):
        print(len(transactions))
        self.timestamp = timestamp
        self.finalSQL = "INSERT INTO transactions_over_time(joined_pool,user_from,user_to) VALUES"
        self.transactions = transactions
        for transaction in transactions:
                self.finalSQL = self.finalSQL + " ('" + self.timestamp + "', '" + transaction['from'] + "', '" + transaction['to'] + "')"


###############DEFINITIONS#############################

url = 'https://www.etherchain.org/txs/data?draw=1&start=0&length=100'

opener = AppURLopener()

conn = psycopg2.connect(host="localhost", database="ethscan", user="postgres", password="cp65482jf")
cur = conn.cursor()

ts = time.localtime()
t2 = datetime.datetime.now()


################FUNCTIONS##############################

##Converts reponse time to current time
def convertTime(transaction):
    if (transaction['time'] == 'a few seconds ago'):
        transaction['time'] = time.strftime("%Y-%m-%d %H:%M:%S", ts)
    else:
        try:
            m = re.search('([0-9]+)( minutes ago)', transaction['time'])
            ago = t2 - datetime.timedelta(minutes=int(m.group(1)))
            transaction['time'] = time.strftime("%Y-%m-%d %H:%M:%S", ago.timetuple())
        except Exception as ex:
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
    transaction = convertTime(transaction)
    transaction = getUsers(transaction)
    return transaction


##Inserts transaction (pending) in DB

def insertIntoDB(transaction):
    sql = "INSERT INTO transactions_over_time (joined_pool, user_from, user_to) VALUES ( '" + transaction['time'] + \
          "','" + transaction['from'] + "','" + transaction['to'] + "')"
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        conn.rollback()
        print(ex)


def routine():
    try:
        response = json.loads(opener.open(url).read().decode())['data']
    except Exception as ex:
        print(ex)
        time.sleep(5)
        return routine()

    for transaction in response:
        print("Starting to process new transaction")
        transaction = cleanData(transaction)
        print("Data cleaned")
        insertIntoDB(transaction)
        print("Inserted into DB")
#######################################################
while True:
    print("Started routine at " + time.strftime("%Y-%m-%d %H:%M:%S", ts))
    routine()
    print("Routine is over")
