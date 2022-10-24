#! /usr/bin/env python

import sys
from os import system
import warnings
import argparse
from datetime import datetime
from pymongo import MongoClient  # sudo pip3 pymongo


warnings.filterwarnings("ignore")

_CONTEXT = 'local'  # aws or local

_NOW = datetime.now()
_TODAY_DATE = str(_NOW.strftime("%Y-%m-%d"))
_TODAY_HOUR = str(_NOW.strftime("%H"))

_MONGODB_HOST = 'localhost'
_MONGODB_PORT = 27017
_MONGODB_USERNAME = 'python'
_MONGODB_PASSWORD = 'python-pwd'
_MONGODB_AUTH_SOURCE = 'twitter'
_MONGODB_COLLECTION_HDFS = 'twitter.hashtags_hdfs'
_MONGODB_COLLECTION_STORM = 'twitter.hashtags_storm'


class Colors:
    OK   = '\033[92m'
    FAIL = '\033[91m'
    END  = '\033[0m'
    BOLD = '\033[1m'


# _____________________________________________________________________
# init_args
def init_args():
    global _NOW

    # my_args = None

    if len(sys.argv) == 1:
        class _Args:
            def __init__(self, date, hour):
                self.date = date
                self.hour = hour
        now = _NOW
        my_args = _Args(
            now.strftime("%Y-%m-%d"),
            now.strftime("%H")
        )
        print('No command arguments provided.')
        print('  > Processing current date and hour.')
        print('  > --date = ' + my_args.date + '   --hour = ' + str(my_args.hour))

    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--date', type=lambda s: datetime.strptime(s, "%Y-%m-%d"), required=True)
        parser.add_argument('--hour', choices=[
            '00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
            '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
            '20', '21', '22', '23'
        ], required=True)
        my_args = parser.parse_args()
        my_args.date = my_args.date.strftime("%Y-%m-%d")

        print('Command arguments provided.')
        print('  > --date = ' + my_args.date + '   --hour = ' + str(my_args.hour))

    return my_args


# _____________________________________________________________________
# main
def main():
    global _CONTEXT
    global _MONGODB_HOST, _MONGODB_PORT
    global _MONGODB_USERNAME, _MONGODB_PASSWORD, _MONGODB_AUTH_SOURCE
    global _MONGODB_COLLECTION_HDFS, _MONGODB_COLLECTION_STORM
    global _TODAY_DATE, _TODAY_HOUR

    system('clear')

    print('************************************************************************************************')
    print('[[ ' + Colors.BOLD + 'STARTING OpenClassRooms PROJET N°5' + Colors.END + ' | CONTEXT = ' + _CONTEXT + ' ]]')
    print('')

    args = init_args()

    mongodb = MongoClient(
        host=_MONGODB_HOST,
        port=_MONGODB_PORT,
        username=_MONGODB_USERNAME,
        password=_MONGODB_PASSWORD,
        authSource=_MONGODB_AUTH_SOURCE
    )
    collection = mongodb[_MONGODB_AUTH_SOURCE][_MONGODB_COLLECTION_HDFS]

    if _TODAY_DATE == args.date and _TODAY_HOUR == args.hour:
        collection = mongodb[_MONGODB_AUTH_SOURCE][_MONGODB_COLLECTION_STORM]

        print('  > STORM PATH')

        records = collection.aggregate([
            {"$match": {"date": args.date, "hour": args.hour}},
            {"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ])

        i = 1
        print('')
        print('------------------------------------------------------------------------')
        print("{0:8} {1:31} {2:10} {3:15} {4:8}".format('RANK', 'HASHTAG', 'COUNT', 'DATE', 'HOUR'))
        print('------------------------------------------------------------------------')
        for rec in records:
            # print(rec)
            print("{0:8} #{1:30} {2:10} {3:15} {4:8}".format(
                i,
                rec['_id'],
                str(rec['count']),
                args.date,
                args.hour,
            ))
            i += 1
        print('------------------------------------------------------------------------')
        print('')

    else:
        print('  > HDFS PATH')

        records = collection.find({"date": args.date, "hour": args.hour}).sort("rank", 1).limit(10)

        print('')
        print('------------------------------------------------------------------------')
        print("{0:8} {1:31} {2:10} {3:15} {4:8}".format('RANK', 'HASHTAG', 'COUNT', 'DATE', 'HOUR'))
        print('------------------------------------------------------------------------')
        for rec in records:
            # print(rec)
            print("{0:8} #{1:30} {2:10} {3:15} {4:8}".format(
                str(rec['rank']),
                rec['hashtag'],
                str(rec['count']),
                rec['date'],
                rec['hour'],
            ))
        print('------------------------------------------------------------------------')
        print('')

    # list the database
    # for database in client.list_databases():
    #     print(database)

    # list the collections
    # for collection_name in client['twitter'].list_collection_names():
    #     print(collection_name)

    # list items in collection
    # collection = client['twitter']['twitter.hashtags_hdfs']
    # records = collection.find()
    # for record in records:
    #     print(record)

    # aggregate = collection.aggregate([
    #     {"$match": {"date": query_date, "hour": query_hour}},
    #     {"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},
    #     {"$sort": {"count": -1}},
    #     {"$limit": 10}
    # ])
    # aggregate = collection.aggregate([
    #     {"$match": {"date": "2021-10-26", "hour": "15"}},
    #     {"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},
    #     {"$sort": {"count": -1}},
    #     {"$limit": 10}
    # ])
    # for agg in aggregate:
    #     print(agg)


if __name__ == "__main__":
    main()
