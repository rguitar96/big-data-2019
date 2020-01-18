#   Created by @RGuitar96 using @sethoscope heatmap and Tweepy library for the Twitter API

#   Dependencies:
#   pip install tweepy
#   pip install textblob

import json
import codecs
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

ckey = 'ckey'
csecret = 'csecret'
atoken = 'atoken'
asecret = 'asecret'

# First pair (longitude, latitude) indicates the lower left or southwest corner
# Second pair indicates the upper right or northeast corner
# alcalá de henares
# region = [-3.406054, 40.462477, -3.335267, 40.521660]
# comunidad de madrid
region = [-11.949371, 35.650688, 4.354341, 44.144845]

class listener(StreamListener):
    def __init__(self):
        super(listener,self).__init__()
        self.i = 0
    def on_status(self, status):
        try:
            #decoded = json.loads(status)

            file =  codecs.open('tweets/%i.txt' % self.i, 'a', "utf-8")
            self.i = self.i + 1
            file.write(str(json.dumps(status._json)))
        except Exception as e:
            print(e)
            return True
    def on_error(self, status_code):
        print(status_code)
        return False

if __name__ == '__main__':
    print('Stream has began...')

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())
twitterStream.filter(locations=region)

#   When you are done capturing tweets, you can generate a heatmap
#   as shown below. You can adjust the parameters as you prefer.
#   
#   python heatmap.py -o output.png --osm -W 2000 
#                     -v -e 39.8591,-4.6506,41.2262,-2.9432 
#                     -d 0.6 -r 60 http://b.tile.stamen.com/toner tweets_coordinates.txt
