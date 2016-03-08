"""This program is used to parse tweets and select information for locals. We only use
geotagged tweets, with user's home information included. """

import pandas as pd
import re
from datetime import datetime 
from dateutil import tz
import json
import numpy as np
import os
import sys
import time

# Converting to local time to find out when people tweet
from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

def screen_geo(tf):
  times = []
  lons=[]
  lats=[]
  langs=[]
  locations_prof=[]
  texts=[]
  names=[]
  locations_at_displayed=[] 
  locations_at_name=[]
  locations_at_type=[]
  usernames=[]
  usertypes=[]

  dt = []

  tweet_count=0
  with open(tf, 'r') as f:
    for line in f.readlines():
      tweet_count += 1
      print '*********** Looking at tweet ***********: ', tweet_count
      line_object = json.loads(line)

      # finding if the user is new yorker or visitor 
      bors=[ '.*queens.*', '.*brooklyn.*', '.*manhattan.*', '.*bronx.*', '.*harlem.*', '.*nyc.*', '.*new\s.*york.*',
             '.*big\s.*apple.*', '.*never\s.*sleeps.*', '.*lower\s.*east\s.*side.*', '.*soho.*', '.*staten\s.*island.*', '.*astoria.*']
      residence=line_object['actor']['location']['displayName']
      match_count=0
      d=0
      for k in bors:
        match = re.search(k, residence, re.IGNORECASE)
        d+=1  
        if match:                      
          match_count+=1             

      if match_count>0: 
        if  'geo' in line_object: 
          if  line_object['geo'] ['coordinates'] is not None and (len(str(line_object['geo']['coordinates'][0]))> 8 and len(str(line_object['geo']['coordinates'][1]))>9):
            if  'location' in line_object:
              locations_at_displayed.append(line_object['location']['displayName'])
              locations_at_name.append(line_object['location']['name'])
              locations_at_type.append(line_object['location']['twitter_place_type'])
            else:
              locations_at_type.append(np.nan)
              locations_at_name.append(np.nan)
              locations_at_displayed.append(np.nan)
            texts.append(line_object['body'])
            times.append(line_object['object']['postedTime'])
            # exctracting yr, mon, day, hr
            tm=line_object['object']['postedTime']
            utc = datetime.strptime(tm, '%Y-%m-%dT%H:%M:%S.000Z')
            print ' UTC ', utc.weekday(), utc.hour, utc.day, utc.year

            # Tell the datetime object that it's in UTC time zone since 
            # datetime objects are 'naive' by default
            utc = utc.replace(tzinfo=from_zone)

            # Convert time zone
            lt = utc.astimezone(to_zone)            
            dt.append(lt)

            locations_prof.append(line_object['actor']['location']['displayName'])
            langs.append(line_object['twitter_lang'])
            usernames.append(line_object['actor']['preferredUsername'])
            usertypes.append(line_object['actor']['objectType'])  
            lats.append(line_object['geo']['coordinates'][0])
            lons.append(line_object['geo']['coordinates'][1])
 

 # Add to Pandas dataframe  
  df_tweets = pd.DataFrame({
      'location_prof': locations_prof,
      'location_at_type': locations_at_name,
      'location_at_name': locations_at_type,
      'location_at_displayed': locations_at_displayed,
      'username': usernames,
      'usertype': usertypes,
      'time': times,
      'language': langs,
      'lons': lons,
      'lats': lats,
      'dt': dt
      })
 

# Change the character encoding to utf-8 
  for col in df_tweets.columns.values:
    if df_tweets[col].dtype=='object':
      df_tweets[col]=df_tweets[col].str.encode('utf-8', errors='ignore')
      
  df_tweets.to_csv("tweets_LOCAL_and_geo.csv")

  f.close()
  f = None
  print min(lons), min(lats)
  print max(lons), max(lats)
def main():
  screen_geo('CustomHPTJob-combined.json')


if __name__ == '__main__':
  main()
