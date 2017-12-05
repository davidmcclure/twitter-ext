

import re
import os
import scandir
import csv
import yaml

from pyspark import SparkContext
from pyspark.sql import SparkSession

from string import punctuation


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


def try_or_none(f):
    """Wrap a class method call in a try block. On error return None.
    """
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            return None
    return wrapper


def clean_tweet(text):
    """Remove links, mentions, and hashtags.
    """
    # Remove links.
    text = re.sub('http\S+', '', text)

    # Remove hashtags and @'s.
    text = re.sub('(#|@)\w+', '', text)

    # Remove linebreaks.
    text = re.sub('[\r\n]+', ' ', text)

    text = text.lower()

    return text


def read_yaml(root, *path_parts):
    """Parse a YAML file relative to the passed path.

    Args:
        root (str)
        path_parts (list of str)

    Returns: dict
    """
    path = os.path.join(os.path.dirname(root), *path_parts)

    with open(path, 'r') as fh:
        return yaml.load(fh)
