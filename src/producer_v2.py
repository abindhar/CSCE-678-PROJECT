# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 19:30:13 2019
Author:Abindu
Changes:
Store only tweets for one topic = "C++"
"""

import json
from kafka import SimpleProducer, KafkaClient
import tweepy
import configparser

# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.

class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka"""   
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True,
                          batch_send_every_n = 1000,
                          batch_send_every_t = 10)

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        print('ok')
        msg =  status.text.encode('utf-8')
        try:
            self.producer.send_messages('twitterstream', msg)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer",status_code)
        return True # Don't kill the stream

    def on_timeout(self):
        print("Timeout occured")
        return True # Don't kill the stream

if __name__ == '__main__':

    # Read the credententials from 'twitter-app-credentials.txt' file
    config = configparser.ConfigParser()
    consumer_key = 'UopwaRDXmyG56bzjgxZwC6o9y'
    consumer_secret = 'j3eFs3PQwPLJGR263viSKutHYhpxP4EF1Sv9MAyevakWDluSVU'
    access_key = '90159436-mhrgUtdfmfeP8yrd1MYgcMtSB27tPPB5u4nPFWqpp'
    access_secret = 'wGjnX9zdk5zl0zfpqhOdxD15mANy5c3tBmz5iAEf8lTSt'

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)                
    api = tweepy.API(auth)

    
    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))                    #Note: use verify = False (in case of OpenSSL error)

    #Custom Filter rules pull all traffic for those filters in real time.
    stream.filter(track = ['DataScience','Avengers','BigData'], languages = ['en'])
    #stream.filter(locations = [-122.75,36.8,-121.75,37.8], languages = ['en'])
