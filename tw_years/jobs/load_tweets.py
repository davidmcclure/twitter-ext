

import click
import bz2
import ujson

from tw_years import fs
from tw_years.utils import scan_paths, get_spark
from tw_years.models import Tweet


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
@click.option('--in_dir', default='s3a://twitter-ia/2017/06/01/00')
@click.option('--out_dir', default='s3a://twitter-years/tweets.paruet')
def main(in_dir, out_dir):
    """Ingest tweets.
    """
    sc, spark = get_spark()

    paths = sc.parallelize(fs.scan(in_dir, '\.json'))

    rows = paths.flatMap(parse_minute)

    df = spark.createDataFrame(rows, Tweet.schema)

    df.write.mode('overwrite').parquet(out_dir)


if __name__ == '__main__':
    main()
