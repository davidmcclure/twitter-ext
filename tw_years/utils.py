

import re
import os
import scandir
import csv

from pyspark import SparkContext
from pyspark.sql import SparkSession


def get_spark():
    """Build sc and spark.
    """
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    return sc, spark


def scan_paths(root, pattern=None):
    """Walk a directory and yield file paths that match a pattern.

    Args:
        root (str)
        pattern (str)

    Yields: str
    """
    for root, dirs, files in scandir.walk(root, followlinks=True):
        for name in files:

            # Match the extension.
            if not pattern or re.search(pattern, name):
                yield os.path.join(root, name)


def dump_csv(rows, path, colnames):
    """Dump rows to a CSV file.

    Args:
        rows (list): List of row tuples.
        path (str): Output path.
        colnames (list): Header row names.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, 'w') as fh:

        writer = csv.writer(fh)

        # Write header row.
        writer.writerow(colnames)

        for row in rows:
            writer.writerow(row)
