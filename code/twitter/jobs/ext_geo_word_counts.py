

import click
import re

from wordfreq import top_n_list

from twitter.utils import get_spark


whitelist = set(top_n_list('en', 10000))


def tokenize_tweet(text):
    """Tokenize tweet text.
    """
    # Remove URLs.
    text = re.sub('http\S+', '', text)

    return re.findall('[a-z0-9#@]+', text.lower())


def count_tokens(tweet):
    """Generate (token, minute) keys.
    """
    for token in tokenize_tweet(tweet.text):
        if token in whitelist:
            yield ((tweet.key, token), 1)


def flatten_row(row):
    (key, token), count = row
    return key, token, count


@click.command()
@click.option('--src', default='data/states.parquet')
@click.option('--dest', default='data/state-word-counts.json')
def main(src, dest):
    """Extract (key, token, count) tuples.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    counts = tweets.rdd \
        .flatMap(count_tokens) \
        .reduceByKey(lambda a, b: a + b) \
        .map(flatten_row) \
        .toDF(('key', 'token', 'count'))

    counts.write \
        .mode('overwrite') \
        .json(dest)


if __name__ == '__main__':
    main()
