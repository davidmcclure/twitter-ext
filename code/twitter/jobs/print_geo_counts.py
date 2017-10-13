

import click

from twitter import fs
from twitter.utils import get_spark


@click.command()
@click.option('--src', default='data/cities.parquet')
def main(src):
    """Count cities.
    """
    sc, spark = get_spark()

    df = spark.read.parquet(src)

    df.groupBy('key').count().orderBy('count', ascending=False).show(100)


if __name__ == '__main__':
    main()
