

import click

from wordfreq import top_n_list

from twitter.utils import get_spark
from twitter.models import Tweet


whitelist = set(top_n_list('en', 10000))


def count_tokens(tweet):
    """Generate (token, month, minute) keys.
    """
    for token in tweet.tokens():
        if token in whitelist:

            month = tweet.posted_time.month
            minute = tweet.posted_time.minute

            yield (token, month, minute), 1


@click.command()
@click.option('--src', default='data/tweets.parquet')
@click.option('--dest', default='data/token-month-minute-counts.json')
def main(src, dest):
    """Extract (token, month, minute, count) tuples.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    counts = tweets.rdd \
        .map(Tweet.from_rdd) \
        .filter(lambda t: t.actor.language == 'en') \
        .flatMap(count_tokens) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda r: (*r[0], r[1])) \
        .toDF(('token', 'month', 'minute', 'count'))

    counts.write \
        .mode('overwrite') \
        .json(dest)


if __name__ == '__main__':
    main()
