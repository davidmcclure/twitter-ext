

import click
import re

from twitter.utils import get_spark


YEAR_PATTERN = '([^0-9a-z_]|^)(?P<year>1[5-9][0-9]{2})([^0-9a-z_]|$)'


def match_years(text, padding=30):
    """Match 4-digit years, 1500-1999.
    """
    for match in re.finditer(YEAR_PATTERN, text):

        c1 = match.start('year')
        c2 = match.end('year')

        c1p = max(c1-padding, 0)
        c2p = min(c2+padding, len(text))

        prefix = text[c1p:c1]
        suffix = text[c2:c2p]

        yield prefix, match.group('year'), suffix


@click.command()
@click.option('--src', default='data/tweets.parquet')
@click.option('--dest', default='data/years.json')
def main(src, dest):
    """Extract 4-digit year candidates.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    matches = tweets.rdd \
        .filter(lambda t: t.actor.language == 'en') \
        .flatMap(lambda t: match_years(t.body)) \
        .toDF(('prefix', 'year', 'suffix'))

    matches.write \
        .mode('overwrite') \
        .json(dest)


if __name__ == '__main__':
    main()
