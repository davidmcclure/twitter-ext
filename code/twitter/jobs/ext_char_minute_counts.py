

import click

from twitter.utils import get_spark
from twitter.models import Tweet


def count_chars(tweet):
    """Generate (char, minute) keys.
    """
    body = tweet.body.encode('ascii', 'ignore')

    for char in str(body):
        yield (char, tweet.posted_time.minute), 1


@click.command()
@click.option('--src', default='data/tweets.parquet')
@click.option('--dest', default='data/char-minute-counts.json')
def main(src, dest):
    """Extract (char, minute, count) tuples.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    counts = tweets.rdd \
        .map(Tweet.from_rdd) \
        .filter(lambda t: t.actor.language == 'en') \
        .flatMap(count_chars) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda r: (*r[0], r[1])) \
        .toDF(('char', 'minute', 'count'))

    counts.write \
        .mode('overwrite') \
        .json(dest)


if __name__ == '__main__':
    main()
