

import click

from twitter import fs
from twitter.utils import get_spark


@click.command()
@click.option('--tweet_dir', default='data/tweets.parquet')
def main(tweet_dir):
    """Count tweets.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(tweet_dir)
    print(tweets.count())


if __name__ == '__main__':
    main()
