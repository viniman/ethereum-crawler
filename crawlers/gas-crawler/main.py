import crawler
from pandas_wrap import Pandas


def main():
    df = Pandas(False).transaction_dataframe
    eth_crawler = crawler.Crawler(df)
    eth_crawler.start()


if __name__ == '__main__':
    main()
