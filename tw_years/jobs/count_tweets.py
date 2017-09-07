

import click

from tw_years import fs
from tw_years.utils import get_spark


@click.command()
@click.option('--tweet_dir', default='tweets.parquet')
def main(tweet_dir):
    """Count tweets.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(tweet_dir)
    print(tweets.count())


if __name__ == '__main__':
    main()
