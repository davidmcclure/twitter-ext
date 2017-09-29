

import click
import bz2
import ujson

from twitter import fs
from twitter.utils import get_spark
from twitter.models import Tweet


def parse_minute(path):
    """Parse a raw minute file.
    """
    with bz2.open(fs.read(path)) as fh:
        for line in fh.readlines():

            raw = ujson.loads(line)

            if 'delete' in raw:
                continue

            yield Tweet.from_api_json(raw)


@click.command()
@click.option('--src', default='data/twitter-ia')
@click.option('--dest', default='data/tweets.parquet')
def main(src, dest):
    """Ingest tweets.
    """
    sc, spark = get_spark()

    paths = sc.parallelize(fs.scan(src, '\.json'))

    rows = paths.flatMap(parse_minute)

    df = spark.createDataFrame(rows, Tweet.schema)

    df.write.mode('overwrite').parquet(dest)


if __name__ == '__main__':
    main()
