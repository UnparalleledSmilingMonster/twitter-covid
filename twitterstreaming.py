import tweepy
from tweepy import StreamingClient, StreamRule
import time

#Used for authentication
bearer_token = "AAAAAAAAAAAAAAAAAAAAAMh9hwEAAAAAwtbFTpiXgy8WIcnoS4KoQGifIOg%3DT9LYVq3wKjEvtSX53mgJzh9idSESs5eEA3GiozDs0V0Q2ZxvaK"
 
class TweetStreamer(tweepy.StreamingClient):

    #If connection to the server is successful
    def on_connect(self):
        print("Connected to the Twitter Server")
        
    #Print out the tweets to terminal with 5s interval
    def on_tweet(self, tweet):
        print(f"{tweet.text}")
        print("-"*50)
        time.sleep(5)
    
    #In case of errors
    def on_errors(self, status):
        raise ValueError('An error has occured while streaming tweets')
        print(status.text)
        
    #Try and get the whole tweet text
    def on_status(self, status):
        if hasattr(status, "retweeted_status"): 
            try:
                print(status.retweeted_status.extended_tweet["full_text"])
            except AttributeError:
                print(status.retweeted_status.text)
        else:
            try:
                print(status.extended_tweet["full_text"])
            except AttributeError:
                print(status.text)

#Create a streaming object
stream = TweetStreamer(bearer_token)

#Add search query
rule = StreamRule(value="covid")
stream.add_rules(rule)

#Start the stream
stream.filter()