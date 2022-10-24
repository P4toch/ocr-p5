#! /usr/bin/env python

import sys
from os import system
from io import BytesIO
import argparse
from datetime import datetime, timedelta

import json
import fastavro  # sudo pip3 fastavro
import moment # sudo pip3 moment
import warnings

from pyspark import SparkContext
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

from pymongo import MongoClient  # sudo pip3 pymongo


""""
docker exec -it namenode /bin/bash
hdfs dfsadmin -safemode leave
hdfs dfs -ls /
hdfs dfs -rm -r /+tmp /hashtags-avro /logs

/home/pat/OCR/projet4/code/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
  --conf spark.rpc.askTimeout=600s \
  --conf spark.driver.memory=2g \
  --packages org.apache.spark:spark-avro_2.12:3.1.2,com.amazonaws:aws-java-sdk:1.12.24,org.apache.hadoop:hadoop-aws:3.2.2 \
  /home/pat/OCR/projet5/query-hdfs.py
"""

warnings.filterwarnings("ignore")

_CONTEXT = 'local'  # aws or local

_DEFAULT_PROCESSING_DAYS = 3

_NOW = datetime.now()
_TODAY_DATE = str(_NOW.strftime("%Y-%m-%d"))
_TODAY_HOUR = str(_NOW.strftime("%H"))

_HDFS_HOST = 'localhost'
_HDFS_PORT = 9000

_MONGODB_HOST = 'localhost'
_MONGODB_PORT = 27017
_MONGODB_USERNAME = 'python'
_MONGODB_PASSWORD = 'python-pwd'
_MONGODB_AUTH_SOURCE = 'twitter'
_MONGODB_COLLECTION = 'twitter.hashtags_hdfs'


class Colors:
    OK   = '\033[92m'
    FAIL = '\033[91m'
    END  = '\033[0m'
    BOLD = '\033[1m'


# _____________________________________________________________________
# init_args
def init_args():
    global _DEFAULT_PROCESSING_DAYS, _NOW

    # my_args = None

    if len(sys.argv) == 1:
        class _Args:
            def __init__(self, from_date, to_date):
                self.from_date = from_date
                self.to_date = to_date
        now = _NOW
        three_days_ago = now - timedelta(days=_DEFAULT_PROCESSING_DAYS - 1)
        my_args = _Args(
            three_days_ago.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d")
        )
        print('No command arguments provided.')
        print('  > Processing last ' + str(_DEFAULT_PROCESSING_DAYS) + ' days (_DEFAULT_PROCESSING_DAYS).')
        print('  > --from_date = ' + my_args.from_date + '   --to_date = ' + my_args.to_date)

    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--from_date', type=lambda s: datetime.strptime(s, "%Y-%m-%d"), required=True)
        parser.add_argument('--to_date',   type=lambda s: datetime.strptime(s, "%Y-%m-%d"), required=True)
        my_args = parser.parse_args()
        if my_args.from_date <= my_args.to_date:
            my_args.from_date = my_args.from_date.strftime("%Y-%m-%d")
            my_args.to_date = my_args.to_date.strftime("%Y-%m-%d")
            print('Command arguments provided.')
            print('  > --from_date = ' + my_args.from_date + '   --to_date = ' + my_args.to_date)
        else:
            print('Arguments provided.')
            print("  ERROR: from_date '" + my_args.from_date.strftime("%Y-%m-%d") +
                  "' greater than to_date '" + my_args.to_date.strftime("%Y-%m-%d") + "' !")
            exit()

    return my_args


# _____________________________________________________________________
# dates_between_two_dates
def dates_between_two_dates(start_date, end_date):
    diff = abs(start_date.diff(end_date).days)

    for n in range(0, diff + 1):
        yield start_date.strftime("%Y-%m-%d")
        start_date = start_date.add(days=1)


# _____________________________________________________________________
# hdfs_path_exists
def hdfs_path_exists(sc, hdfs_path):
    try:
        rdd_tmp = sc.textFile(hdfs_path)
        rdd_tmp.take(1)
        return True
    except Py4JJavaError as e:
        return False


# _____________________________________________________________________
# main
def main_():
    global _CONTEXT
    global _HDFS_HOST, _HDFS_PORT
    global _MONGODB_HOST, _MONGODB_PORT
    global _MONGODB_USERNAME, _MONGODB_PASSWORD, _MONGODB_AUTH_SOURCE, _MONGODB_COLLECTION

    system('clear')

    sc = SparkContext()
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    mongodb = MongoClient(
        host=_MONGODB_HOST,
        port=_MONGODB_PORT,
        username=_MONGODB_USERNAME,
        password=_MONGODB_PASSWORD,
        authSource=_MONGODB_AUTH_SOURCE
    )
    collection = mongodb[_MONGODB_AUTH_SOURCE][_MONGODB_COLLECTION]

    # records = collection.find({"date": "2021-11-16", "hour": "12"}).sort("rank", 1).limit(10)
    # print("{0:31} {1}".format('HASHTAG', 'COUNT'))
    # for rec in records:
    #     # print(rec)
    #     print("#{0:30} {1}".format(rec['hashtag'], str(rec['count'])))
    # exit()

    system('clear')

    print('************************************************************************************************')
    print('[[ ' + Colors.BOLD + 'STARTING OpenClassRooms PROJET N°5' + Colors.END + ' | CONTEXT = ' + _CONTEXT + ' ]]')
    print('')

    my_args = init_args()

    print('')

    from_date = moment.date(my_args.from_date)
    to_date   = moment.date(my_args.to_date)

    avro_schema = """
    {
       "namespace": "ocr.p5",
       "name": "hashtags",
       "type": "record",
       "fields" : [
            { "name" : "unique_id", "type" : "string" },
            { "name" : "date", "type" : "string" },
            { "name" : "hour", "type" : "string" },
            { "name" : "hashtag", "type" : "string" }
       ]
    }
    """

    for date in dates_between_two_dates(from_date, to_date):
        print('Starting to aggregate HDFS avro files for date : ' + Colors.BOLD + date + Colors.END)

        hdfs_path = 'hdfs://' + _HDFS_HOST + ':' + str(_HDFS_PORT) + '/hashtags-avro/date=' + date
        # print(hdfs_path)

        if hdfs_path_exists(sc, hdfs_path):
            print('    ' + Colors.OK + hdfs_path + '/*.avro' + Colors.END)

            rdd = sc.binaryFiles(
                hdfs_path + '/*.avro') \
                .flatMap(lambda args: fastavro.reader(BytesIO(args[1])))
              # .flatMap(lambda args: fastavro.reader(BytesIO(args[1]), reader_schema=avro_schema))

            df = spark.createDataFrame(rdd)
            df.cache()  # df.persist()

            hour_list = [
                '00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
                '20', '21', '22', '23']
            # hour_list = ['11', '12', '13', '14', '15']

            for hour in hour_list:
                print('  > ' + date + ':' + hour + 'H     --->     Update MongoDB: ', end='')

                df_tmp = df.where("hour = '" + hour + "'")
                df_tmp.cache()
                df_tmp.createOrReplaceTempView("df_tmp")

                query = """
                    select
                        date,
                        hour,
                        row_number() over(order by count(hashtag) desc, hashtag asc) as rank,
                        hashtag,
                        count(hashtag) as count
                    from df_tmp
                    group by
                        date,
                        hour,
                        hashtag
                    limit 10
                """

                # spark.sql(query).show(10, truncate=False)

                my_json = spark.sql(query) \
                    .toDF('date', 'hour', 'rank', 'hashtag', 'count') \
                    .toJSON() \
                    .map(lambda j: json.loads(j)) \
                    .collect()
                # print(my_json)

                collection.delete_many({"date": date, "hour": hour})
                if len(my_json) > 0:
                    collection.insert_many(my_json)
                    print(Colors.OK + 'Done' + Colors.END)
                else:
                    print(Colors.FAIL + 'No data' + Colors.END)

                # spark.catalog.uncacheTable('df_tmp')
                # df_tmp.unpersist()

        else:
            print('    ' + Colors.FAIL + hdfs_path + '/*.avro' + Colors.END)
            print('    ' + Colors.FAIL + 'Not found' + Colors.END)

        print('')


if __name__ == "__main__":
    main_()
