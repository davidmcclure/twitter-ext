

import click

from twitter import fs
from twitter.models import CityTweet
from twitter.utils import get_spark


# https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
cities = [
    'new york',
    'los angeles',
    'chicago',
    'houston',
    'phoenix',
    'philadelphia',
    'san antonio',
    'san diego',
    'dallas',
    'san jose',
    'austin',
    'jacksonville',
    'san francisco',
    'columbus',
    'indianapolis',
    'fort worth',
    'charlotte',
    'seattle',
    'denver',
    'el paso',
    'washington',
    'boston',
    'detroit',
    'nashville',
    'memphis',
    'portland',
    'oklahoma city',
    'las vegas',
    'louisville',
    'baltimore',
    'milwaukee',
    'albuquerque',
    'tuscon',
    'fresno',
    'sacramento',
    'mesa',
    'kansas city',
    'atlanta',
    'long beach',
    'colorado springs',
    'raleigh',
    'miami',
    'virginia beach',
    'omaha',
    'oakland',
    'minneapolis',
    'tulsa',
    'arlington',
    'new orleans',
    'wichita',
]


def match_city(tweet):
    """Match (city, text) tuples.
    """
    for city in cities:
        if city in tweet.user.location.lower():
            return CityTweet(city, tweet.user.location, tweet.text)


@click.command()
@click.option('--tweet_dir', default='data/tweets.parquet')
@click.option('--result_path', default='data/cities.parquet')
def main(tweet_dir, result_path):
    """Get tweets for cities, using (stupid) string matching.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(tweet_dir)

    matches = tweets.rdd \
        .filter(lambda t: t.user.location) \
        .map(match_city) \
        .filter(bool) \
        .toDF(CityTweet.schema)

    matches.write \
        .mode('overwrite') \
        .parquet(result_path)

    print(matches.count())


if __name__ == '__main__':
    main()
