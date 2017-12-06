

import re
import os
import scandir
import csv
import yaml

from pyspark import SparkContext
from pyspark.sql import SparkSession

from string import punctuation


TOKEN_RE = re.compile(u'[\w'
    u'\U0001F300-\U0001F64F'
    u'\U0001F680-\U0001F6FF'
    u'\u2600-\u26FF\u2700-\u27BF]+',
re.UNICODE)


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
    # Remove links, hashtags, @s.
    text = re.sub('http\S+', '', text)
    text = re.sub('(#|@)\w+', '', text)

    # Match chars + emoji.
    tokens = re.findall(TOKEN_RE, text.lower())

    return ' '.join(tokens)


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
