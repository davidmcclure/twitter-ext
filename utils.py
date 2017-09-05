

import re
import os
import scandir

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
