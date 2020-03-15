from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import credentials
from pymongo import MongoClient
import shapefile
from shapely.geometry import Point, Polygon
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# coordinates of Melbourne
melbourne_locations = [144.946457, -37.840935, 145.9631, -36.8136]
sf = shapefile.Reader("/Users/pengkedi/PycharmProjects/realtime-sentiment/1270055001_sa2_2016_aust_shape/SA2_2016_AUST")
records = sf.records()
shapes = sf.shapes()
length = len(records)


class StreamListener(StreamListener):

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            store_tweet(tweet)
        except BaseException as e:
            print("Error on data: %s " % str(e))

    def on_error(self, status_code):
        if status_code == 420:
            return False


def stream_tweets(locations):
    listener = StreamListener()
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)

    stream = Stream(auth, listener)
    stream.filter(locations=locations)


def store_tweet(tweet):
    place = tweet['place']['name']

    if tweet['coordinates'] is not None:
        coord = tweet['coordinates']
    elif str(place) != "Melbourne" and str(place) != 'New South Wales' and str(place) != 'Victoria':
        coord = tweet['place']['bounding_box']['coordinates'][0][0]
    else:
        coord = None

    sa2_name, sa2_code = get_sa2_info(coord)
    tweet_doc = {'text': tweet['text'],
                'coordinates': coord,
                'created_at': tweet['created_at'],
                'sa2_name': sa2_name,
                'sa2_code': sa2_code,
                'user_id': tweet['user']['id'],
                'sentiment': sentiment_analyzer_scores(tweet['text'])}
    print(tweet_doc)
    db.tweet.insert_one(tweet_doc)


def sentiment_analyzer_scores(text):
    """
    Get sentiment score using Vader, normalize sentiment score to [-1, 1]
    :param text: The sentence to be analyzed
    :return: The sentiment score
    """
    score = SentimentIntensityAnalyzer().polarity_scores(text)
    compound = score['compound']

    if -0.05 < compound < 0.05:
        sentiment = 0
    elif compound >= 0.05:
        sentiment = score['pos']
    elif compound <= -0.05:
        sentiment = -score['neu']

    return sentiment


def get_sa2_info(coordinate):
    """
    get SA2 name and code based on latitude and longtitude.
    :param lon:
    :param lat:
    :return:
    """
    if coordinate is None:
        return "Melbourne", 00000

    p = Point(coordinate[0], coordinate[1])
    for i in range(length):
        poly = Polygon(shapes[i].points)
        if poly.contains(p):
            sa2_name = records[i][2]
            sa2_code = records[i][1]
            return sa2_name, sa2_code


if __name__ == "__main__":
    client = MongoClient('localhost', 27017)
    db = client.test

    stream_tweets(melbourne_locations)
