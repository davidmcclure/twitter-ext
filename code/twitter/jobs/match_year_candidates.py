

import click
import re

from twitter.utils import get_spark


def match_years(text, padding=30):
    """Match 4-digit years.
    """
    for match in re.finditer('[0-9]{4}', text):

        c1 = match.start()
        c2 = match.end()

        c1p = max(c1-padding, 0)
        c2p = min(c2+padding, len(text))

        prefix = text[c1p:c1]
        suffix = text[c2:c2p]

        yield prefix, match.group(0), suffix


@click.command()
@click.option('--tweet_dir', default='data/tweets.parquet')
@click.option('--result_path', default='years.txt')
def main(tweet_dir, result_path):
    """Extract 4-digit year candidates.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(tweet_dir)

    matches = tweets.rdd \
        .flatMap(lambda t: match_years(t.text)) \
        .toDF(('prefix', 'year', 'suffix'))

    matches.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(result_path)


if __name__ == '__main__':
    main()
