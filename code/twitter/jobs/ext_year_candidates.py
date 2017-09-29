

import click
import re

from twitter.utils import get_spark


def match_years(text, padding=30):
    """Match 4-digit years.
    """
    for match in re.finditer('19[0-9]{2}', text):

        c1 = match.start()
        c2 = match.end()

        c1p = max(c1-padding, 0)
        c2p = min(c2+padding, len(text))

        prefix = text[c1p:c1]
        suffix = text[c2:c2p]

        yield prefix, match.group(0), suffix


@click.command()
@click.option('--src', default='data/tweets.parquet')
@click.option('--dest', default='data/years.csv')
def main(src, dest):
    """Extract 4-digit year candidates.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    matches = tweets.rdd \
        .filter(lambda t: t.lang == 'en') \
        .flatMap(lambda t: match_years(t.text)) \
        .toDF(('prefix', 'year', 'suffix'))

    matches.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(dest)


if __name__ == '__main__':
    main()
