#! /usr/bin/env python

import json
import twitter  # pip install twitter
import hashlib

from time import time
from datetime import datetime
# from kafka import KafkaProducer  # sudo pip3 install kafka-python
# https://github.com/confluentinc/confluent-kafka-python
from confluent_kafka import Producer  # sudo pip3 install confluent_kafka requests avro

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer




def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed {} [{}]'.format(msg.topic(), msg.partition()))
        print('Message delivery failed: {}'.format(err))
    # else:
    #   print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def main():

    # producer = KafkaProducer(bootstrap_servers="localhost:9092")
    # producer = Producer({'bootstrap.servers': 'localhost:9092'})
    # producer = KafkaProducer(
    #     security_protocol='SSL',
    #     bootstrap_servers=[
    #         "b-1.kafka-aws.vb6gat.c4.kafka.eu-west-3.amazonaws.com:9094",
    #         "b-2.kafka-aws.vb6gat.c4.kafka.eu-west-3.amazonaws.com:9094",
    #         "b-3.kafka-aws.vb6gat.c4.kafka.eu-west-3.amazonaws.com:9094"
    #     ]
    # )

    # Put here your Twitter API credentials obtained at https://apps.twitter.com/
    # Note: you need a Twitter account to create an app.
    _TWITTER_ACCESS_TOKEN = '--------------------------------'
    _TWITTER_ACCESS_TOKEN_SECRET = '--------------------------------'
    _TWITTER_CONSUMER_TOKEN = '--------------------------------'
    _TWITTER_CONSUMER_TOKEN_SECRET = '--------------------------------'

    oauth = twitter.OAuth(
        _TWITTER_ACCESS_TOKEN,
        _TWITTER_ACCESS_TOKEN_SECRET,
        _TWITTER_CONSUMER_TOKEN,
        _TWITTER_CONSUMER_TOKEN_SECRET
    )

    t = twitter.TwitterStream(auth=oauth)
    sample_tweets_in_french = t.statuses.sample(language='fr')

    avro_schema = """
    {
       "namespace": "ocr.p5",
       "name": "hashtags",
       "type": "record",
       "fields" : [
            { "name" : "unique_id", "type" : "string" },
            { "name" : "timestamp", "type" : "int", "logicalType": "date" },
            { "name" : "date", "type" : "string" },
            { "name" : "hour", "type" : "string" },
            { "name" : "hashtag", "type" : "string" }
       ]
    }
    """
    # print(avro_schema)
    schema = avro.loads(avro_schema)

    avro_producer = AvroProducer({
        'bootstrap.servers': ["172.31.2.27:9092", "172.31.2.27:9093", "172.31.2.27:9094"],
        'on_delivery': delivery_report,
        'schema.registry.url': 'http://localhost:8081'
        }, default_value_schema=schema
    )

    for tweet in sample_tweets_in_french:
        if "delete" in tweet:
            # Deleted tweet events do not have any associated text
            continue

        # print("===================================")

        # Tweet text
        # print(tweet["text"])
        # print(tweet.keys())

        # Collect hashtags
        if "entities" in tweet:
            hashtags = [h['text'] for h in tweet["entities"]["hashtags"]]
            # author_id = tweet['expansions']['author_id']

            if len(hashtags) > 0:
                id_str = tweet['id_str']
                user_name = tweet['user']['name']
                for h in hashtags:
                    now = datetime.now()  # current date and time
                    # date_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    # date_time = datetime.today().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                    timestamp = int(time())
                    date = now.strftime("%Y-%m-%d")
                    hour = now.strftime("%H")
                    if len(hour) == 1:
                        hour = '0' + hour
                    unique_id = hashlib.sha256()
                    unique_id.update((str(id_str)+str(user_name)+str(h)).encode())
                    # print(unique_id.hexdigest())
                    # print(h)
                    msg = {
                        'unique_id': unique_id.hexdigest(),
                        'timestamp': timestamp,
                        'date': date,
                        'hour': hour,
                        # 'date_hour': str(date) + '_' + str(hour) + 'H',
                        'hashtag': h
                    }
                    print(json.dumps(msg, default=str).encode())

                    # producer.poll(0)
                    # producer.produce(
                    #     topic='hashtags-json',
                    #     value=json.dumps(msg, default=str).encode('utf-8'),
                    #     callback=delivery_report)
                    # producer.flush()

                    avro_producer.poll(0)
                    avro_producer.produce(
                        topic='hashtags-avro',
                        value=msg,
                        value_schema=schema)
                    avro_producer.flush()


if __name__ == "__main__":
    main()

