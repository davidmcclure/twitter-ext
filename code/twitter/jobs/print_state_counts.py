

import click

from twitter import fs
from twitter.utils import get_spark


@click.command()
@click.option('--src', default='data/states.parquet')
def main(src):
    """Count states.
    """
    sc, spark = get_spark()

    df = spark.read.parquet(src)

    df.groupBy('state').count().orderBy('count', ascending=False).show(100)


if __name__ == '__main__':
    main()
