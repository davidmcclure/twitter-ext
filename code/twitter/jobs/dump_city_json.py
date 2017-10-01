

import click

from twitter.utils import get_spark


@click.command()
@click.argument('cities', nargs=-1)
@click.option('--src', default='data/cities.parquet')
@click.option('--dest', default='data/cities.json')
@click.option('--coalesce', type=int, default=100)
def main(cities, src, dest, coalesce):
    """Dump selected cities as JSON.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    matches = tweets \
        .filter(tweets.city.isin(set(cities))) \
        .coalesce(coalesce)

    matches.write.mode('overwrite').json(dest)


if __name__ == '__main__':
    main()
