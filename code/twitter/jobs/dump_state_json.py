

import click

from twitter.utils import get_spark


@click.command()
@click.argument('states', nargs=-1)
@click.option('--src', default='data/states.parquet')
@click.option('--dest', default='data/states.json')
@click.option('--coalesce', type=int, default=100)
def main(states, src, dest, coalesce):
    """Dump selected states as JSON.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    matches = tweets \
        .filter(tweets.state.isin(set(states))) \
        .coalesce(coalesce)

    matches.write.mode('overwrite').json(dest)


if __name__ == '__main__':
    main()
