

import click

from operator import add

from twitter import fs
from twitter.utils import get_spark, dump_csv


@click.command()
@click.option('--src', default='data/tweets.parquet')
@click.option('--dest', default='data/text-len-hist.csv')
def main(src, dest):
    """Make a histogram of tweet character counts.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    counts = (
        tweets.rdd
        .map(lambda t: (len(t.text), 1))
        .reduceByKey(add)
        .sortBy(lambda x: x[0])
        .collect()
    )

    dump_csv(counts, dest, ('text_len', 'count'))


if __name__ == '__main__':
    main()
