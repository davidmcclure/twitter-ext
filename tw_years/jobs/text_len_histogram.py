

import click

from operator import add

from tw_years import fs
from tw_years.utils import get_spark, dump_csv


@click.command()
@click.option('--tweet_dir', default='tweets.parquet')
@click.option('--result_path', default='text-len-histogram.csv')
def main(tweet_dir, result_path):
    """Make a histogram of tweet character counts.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(tweet_dir)

    counts = (
        tweets.rdd
        .map(lambda t: (len(t.text), 1))
        .reduceByKey(add)
        .sortBy(lambda x: x[0])
        .collect()
    )

    dump_csv(counts, result_path, ('text_len', 'count'))


if __name__ == '__main__':
    main()