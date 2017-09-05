

import click
import bz2
import ujson

from pyspark import SparkContext
from pyspark.sql import SparkSession

from twitter import Tweet
from utils import scan_paths


def parse_minute(path):
    """Parse a raw minute file.
    """
    with bz2.open(path) as fh:
        for line in fh.readlines():

            raw = ujson.loads(line)

            try:

                yield Tweet(
                    id=raw['id_str'],
                    text=raw['text'],
                    timestamp_ms=raw['timestamp_ms'],
                    lang=raw['lang'],
                )

            except:
                pass


@click.command()
@click.argument('in_dir')
@click.argument('out_dir')
def main(in_dir, out_dir):
    """Ingest tweets.
    """
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    paths = sc.parallelize(scan_paths(in_dir, '\.json'))

    rows = paths.flatMap(parse_minute)

    df = spark.createDataFrame(rows, Tweet.schema)
    df.write.parquet(out_dir)


if __name__ == '__main__':
    main()
