

import click
import re

from pyspark.sql import Row

from twitter import fs
from twitter.utils import get_spark, try_or_none, clean_tweet
from twitter.models import GeoTweet


@click.command()
@click.argument('states', nargs=-1)
@click.option('--src', default='/data/geo-tweets.parquet')
@click.option('--dest', default='/data/corpus.txt')
@click.option('--partitions', default=100)
def main(states, src, dest, partitions):
    """Dump state tweets for glove.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    texts = (
        tweets.filter(tweets.state.isin(set(states)))
        .select('body')
        .rdd.map(lambda r: Row(body=clean_tweet(r['body'])))
        .toDF()
        .coalesce(partitions)
    )

    texts.write.mode('overwrite').text(dest)


if __name__ == '__main__':
    main()
