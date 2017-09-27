

import click

from twitter import fs
from twitter.utils import get_spark


cities = [
    'boston',
    'san francico',
    'los angeles',
    'chicago',
    'dallas',
    'houston',
    'detroit',
    'seattle',
    'new orleans',
    'miami',
    'atlanta',
    'denver',
    'philadelphia',
]


def match_city(tweet):
    """Match (city, text) tuples.
    """
    for city in cities:
        if city in tweet.user.location.lower():
            return (city, tweet.user.location, tweet.text)


@click.command()
@click.option('--tweet_dir', default='data/tweets.parquet')
@click.option('--result_path', default='results/cities/tweets.csv')
def main(tweet_dir, result_path):
    """Get tweets for cities, using (stupid) string matching.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(tweet_dir)

    matches = (
        tweets.rdd
        .filter(lambda t: t.user.location)
        .map(match_city)
        .filter(bool)
        .toDF()
    )

    matches.write.mode('overwrite').csv(result_path)


if __name__ == '__main__':
    main()
