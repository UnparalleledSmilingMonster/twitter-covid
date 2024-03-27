import TwitterAPI
import pandas as pd
import json
import os 
import requests
import time
import shutil
import logging


from requests.exceptions import ConnectionError, ReadTimeout, SSLError
from requests.packages.urllib3.exceptions import ReadTimeoutError, ProtocolError
from enum import Enum
from tqdm import tqdm



from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col,struct,when,lit,udf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType




spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

sc = SparkContext.getOrCreate();

banned_logging = ["pyspark", "py4j"]

logging.basicConfig( filename='scraper.log',
        format= '[%(asctime)s] {%(lineno)d} - %(funcName)s - %(levelname)s : %(message)s',
        level=logging.INFO,
        datefmt='%H:%M:%S')
 
for module in banned_logging:
    logging.getLogger(module).setLevel(logging.ERROR) #Not to get overflown with warn/info logs from other modules


# API keys that yous saved earlier
api_key = "bla"
api_secret = "blabla"
bearer_token = "bla"
access_token = "123"
access_secret = "123456"
 
# Authenticate to Twitter:
api = TwitterAPI.TwitterAPI(api_key, api_secret, auth_type='oAuth2',api_version='2')

if api : print("Successfully authenticated ! \n")
else : print("Connection failed.\n")


format = "%Y-%m-%dT%H:%M:%SZ"

#let's test in on the current date : 
now = datetime.now()
date_time_str = now.strftime(format)

begin = date(2019, 12, 1)  #1st of December 2019

month_stamps = [ (begin + relativedelta(months=i)).strftime(format) for i in range(31)]

months_dic = {1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June", 7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12 : "December"}


def filename_date(ddate):
    return months_dic[ddate.month] + "_" + str(ddate.year)

header = "data/"

def pipe_nline_trimmer(text):
     return text.replace('|','').replace('\n','') 

#using pyspark : creates a directory in which the file is partitionned in multiple smaller files
def append_tweets_spark(tweets_json, filename, schema = False):
    tweets_data = json.dumps(tweets_json['data'])
    df_data=spark.read.option("multiline", "true").json(sc.parallelize([tweets_data]))
    
    text_cleaner = udf(pipe_nline_trimmer)

    df_data = df_data.withColumn("text", text_cleaner(df_data.text))
    df_data.select(col("id").cast(StringType()))
    df_data.select(col("author_id").cast(StringType()))
    
    if 'geo' not in df_data.columns :
        df_data = df_data.withColumn("geo",struct(*[lit(None).cast("string").alias("place_id")]))
        logging.warning("'geo' field missing from the dataframe. Adding it with null values.")
    
    if 'withheld' in df_data.columns:
        df_data = df_data.drop("withheld")
        logging.warning("Removing 'witheld' field from the dataframe")
    
    if schema :
        logging.info("Dataframe schema : \n %s", df_data._jdf.schema().treeString())
   
    #The schema provided by df shows that one column is an array of strings. CSV format doesn't accept it but we are not interested in it. So we discard it. 
    df_data = df_data.drop("edit_history_tweet_ids")
    #We then collapse the geo struct field 
    df_data = df_data.withColumn("place_id", col("geo.place_id")).drop("geo")


    if 'includes' in tweets_json:
        tweets_includes = json.dumps(tweets_json['includes']['places'])    
    else :
        #If there is no place given, we create a random one which won't match anything but will serve to keep the right format.
        logging.warning("'includes' field missing from the json. Adding it with default values (not matching anything)")
        tweets_includes = [{"country": "NotACountry","country_code": "NAC","full_name": "Still Not A Country","id": "-1"}]

    df_includes = spark.read.json(sc.parallelize([tweets_includes]),multiLine=True)
    df_includes = df_includes.drop('full_name')
    df_data = df_data.alias("a").join(df_includes.alias("b"), df_data.place_id == df_includes.id  , "leftouter").select("a.*", "b.country","b.country_code")
    #df_data.show()

    df_data.write.option("header","true").option("delimiter","|").csv(header+filename,mode="append")
    
    
#Note this code is (very) partially adapted from the TwitterPager class from the TwitterAPI library but tweaked to better serve our purpose 
#(New parameters added for a better uniformity of the query result)

class Granularity(Enum):
  Day = 0
  Hour = 1
  Minute = 2

class PagerCount:
   """Class implementation of the paging from Twitter API. Only designed for the Twitter API v2.
    :param api: An authenticated TwitterAPI object
    :param endpoint: The endpoint of the query
    :param params: Dictionary of resource parameters (the query is contained in it)
    :param limit: Defines how many tweets to scrape
    :param granularity: (int) The granularity of the request for slicing
   
   Does not provide hydration as we only scrape original tweets and are not interested in media content.
   """
   def __init__(self, api, endpoint, limit, filename, params=None):
        self.api = api
        self.endpoint = endpoint
        self.params = params if params!=None else {}
        self.limit = limit
        self.filename = filename

   def iterate(self, granularity = None,wait=5, schema = False):
        """Iterate response from Twitter REST API resource. Resource is called
        in a loop to retrieve consecutive pages of results.
        :param granularity : Defines the granularity for the temporal slicing
        :param wait: Floating point number (default=5) of seconds wait between requests.
        :param schema: To log the schema of the dataframe (only logged once)
        """

        assert self.api.version == '2'

        ignore_gran = True if granularity == None else False

        if 'start_time' not in self.params or 'end_time' not in self.params:    #No point in slicing if there is no temporal bounds
            ignore_gran = True

        if not ignore_gran:
            begin = datetime.strptime(str(self.params['start_time']),format)
            end = datetime.strptime(self.params['end_time'],format)
            delta = [int((end- begin).days), int((end- begin).days*24), int((end- begin).days*24*60)]
            samples = delta[granularity.value]
            if samples == 0 : 
                ignore_gran = True #In the case the start and end are in the same day the granularity in day makes no sense e.g.
            else:
                logging.info("The difference in %s is %s", granularity, samples)
                count_per_sample = self.limit // samples
                logging.info("We want to scrape %s tweets per%s", count_per_sample, granularity)
                dic_delta = {Granularity.Day:lambda x : relativedelta(days=x),Granularity.Hour:lambda x : relativedelta(hours=x), Granularity.Minute : lambda x : relativedelta(minutes=x)}
                date_stamps = [(begin + dic_delta[granularity](i)).strftime(format) for i in range(samples+1)]

        
        if ignore_gran :
            #logging.info("No granularity mode")
            count = 0
            elapsed = 0
            exp_backoff = 0 
            while count < self.limit:
                try:
                    # Get one page
                    start = time.time()
                    rq = self.api.request(self.endpoint, self.params)
                    it = rq.get_iterator()

                    json_rq = rq.json()
                    if 'meta' not in json_rq: break
                    meta = json_rq['meta']
                    item_count = meta['result_count']

                    #logging.info("This batch contains : %s tweets", item_count)

                    for item in it:
                        if type(item) is dict:
                            if 'status' in item:
                                if item['status'] in [130, 131, 429]:
                                    # Twitter service error
                                    raise TwitterAPI.TwitterError.TwitterConnectionError(item)
                    

                    if item_count != 0 :
                        append_tweets_spark(json_rq, self.filename, schema)
                        count += item_count

                    exp_backoff = 0 #Reset the exponential backoff once a request went through

                    if not 'next_token' in meta: 
                        logging.info("No next token, stopping.")
                        break

                     # Sleep
                    elapsed = time.time() - start
                    pause = wait - elapsed if elapsed < wait else 0
                    time.sleep(pause)

                    self.params['next_token'] = meta['next_token']


                except TwitterAPI.TwitterError.TwitterRequestError as e:
                    if e.status_code < 500:
                        for msg in iter(e):
                            logging.warning("Error caught %s", msg)
                            
                        if e.status_code== 429 :
                            exp_backoff +=1
                            exp_wait = 2**exp_backoff
                            logging.warning("Waiting %s seconds before retrying",str(exp_wait))
                            time.sleep(exp_wait)
                    continue
                except TwitterAPI.TwitterError.TwitterConnectionError:
                    continue
            
            logging.info(" %s tweets were read.", count)
            return 0 if count >= self.limit else self.limit - count

        
        else : #when there is granularity, use recursivity on PagerCount
            logging.info("Granularity mode enabled")
            m = len(date_stamps)-1
            for i in tqdm(range(m)):
                print("Working on sliced range n°",i+1) 
                params_tuned = self.params.copy()
                params_tuned['start_time'] = date_stamps[i]
                params_tuned['end_time'] = date_stamps[i+1]
                buffer = PagerCount(self.api, self.endpoint, count_per_sample, self.filename, params_tuned).iterate(granularity = None, wait=wait, schema=schema)
                schema = False
                count_per_sample = count_per_sample + buffer // (m-i-1) if i < m-1 else count_per_sample
                time.sleep(1)



n = len(month_stamps)
filename=["" for i in range(n-1)]
for i in range(n-1):
    endpoint = "tweets/search/all"
    params = {'query':'(#Covid19 OR #coronavirus OR #Covid-19 OR #covid) -is:retweet -RT lang:en',
            'tweet.fields':'lang,author_id,created_at',
            'expansions':'geo.place_id',
            'place.fields':'country,country_code',
            'start_time':month_stamps[i],
            'end_time':month_stamps[i+1],
            'max_results':500 }

    tweets_per_month = 100000

    filename_csv = filename_date(datetime.strptime(month_stamps[i],format))+".csv"
    filename_zip = filename_csv[:-4]
    filename[i]=filename_zip+".zip"
    
    logging.info("Starting to scrape %s", filename_csv[:-4])

    scraper = PagerCount(api, endpoint, tweets_per_month, filename_csv, params)
    scraper.iterate(Granularity.Day, wait = 1, schema = True)









