

import os

from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext.getOrCreate()

spark = SparkSession(sc).builder.getOrCreate()
