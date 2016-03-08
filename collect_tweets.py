"""Colecting tweets over a specified geo location using REST API.
"""
import tweepy
import pandas as pd
import re
import datetime
import matplotlib as plt
import json
import pprint
import jsonpickle
import os

api_key = ""
api_secret = ""

# has higher rates than OAuth
auth = tweepy.AppAuthHandler(api_key, api_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
lat=  -34.60
lon= -58.43
string=""
max_range=8.5
#Given a latitude and a longitude pair, an IP address, or a name, this request will return a list of all the valid places that can be used as the place_id wheemacs outn updating a status.
#https://api.twitter.com/1.1/geo/search.json


def get_all_tweets():
	maxTweets = 100000000 # Some arbitrary large number
	tweetsPerQry = 100  # this is the max the API permits
	fName = 'tweets_tango.txt' # Storing the tweets in a text file.

	# If results from a specific ID onwards are reqd, set since_id to that ID.
	# else default to no lower limit, go as far back as API allows
	sinceId = None

	# If results only below a specific ID are, set max_id to that ID.
	# else default to no upper limit, start from the most recent tweet matching the search query.
	max_id = -1L
	tweetCount = 0
	print("Downloading max {0} tweets".format(maxTweets))
	with open(fName, 'w') as f:
		while tweetCount < maxTweets:
			try:
				if (max_id <= 0):
					if (not sinceId):
						new_tweets = api.search(q=string, result_type='recent', count=100, lang='en', geocode = "%f,%f,%dkm" % (lat, lon, max_range)) #lang='en',
					else:
						new_tweets = api.search(q=string, result_type='recent', count=100, lang='en', geocode = "%f,%f,%dkm" % (lat, lon, max_range), since_id=sinceId)
				else:
					if (not sinceId):
						new_tweets = api.search(q=string, result_type='recent', count=100, lang='en', geocode = "%f,%f,%dkm" % (lat, lon, max_range),  max_id=str(max_id - 1))
					else:
						new_tweets = api.search(q=string, result_type='recent', count=100, lang='en', geocode = "%f,%f,%dkm" % (lat, lon, max_range),  max_id=str(max_id - 1),	since_id=sinceId)
				if not new_tweets:
					print("No more tweets found")
					break
				for tweet in new_tweets:
					f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
				tweetCount += len(new_tweets)
				print("Downloaded {0} tweets".format(tweetCount))
				max_id = new_tweets[-1].id
			except tweepy.TweepError as e:
				# Just exit if any error
				print("some error : " + str(e))
				break

	print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))


if __name__ == '__main__':
       	get_all_tweets()
