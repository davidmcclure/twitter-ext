

import click

from twitter.utils import get_spark
from twitter.models import Tweet


def count_tokens(tweet):
    """Generate (token, minute) keys.
    """
    for token in tweet.tokens():
        yield ((token, tweet.posted_time.minute), 1)


def flatten_row(row):
    (token, minute), count = row
    return token, minute, count


@click.command()
@click.option('--src', default='data/tweets.parquet')
@click.option('--dest', default='data/minutes.json')
def main(src, dest):
    """Extract (token, minute, count) tuples.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    counts = tweets.rdd \
        .map(Tweet.from_rdd) \
        .filter(lambda t: t.actor.language == 'en') \
        .flatMap(count_tokens) \
        .reduceByKey(lambda a, b: a + b) \
        .map(flatten_row) \
        .toDF(('token', 'minute', 'count'))

    counts.write \
        .mode('overwrite') \
        .json(dest)


if __name__ == '__main__':
    main()
