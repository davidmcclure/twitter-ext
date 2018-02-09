

import click

from operator import add

from cortico_data.services import spark
from cortico_data.models import Tweet


@click.command()
@click.option('--src', default='/data/tweets.parquet')
def main(src):
    """Get word count for all tweets.
    """
    df = spark.read.parquet(src)

    count = (
        df.rdd
        .map(Tweet.from_rdd)
        .map(lambda t: len(t.tokens()))
        .reduce(add)
    )

    print(count)


if __name__ == '__main__':
    main()
