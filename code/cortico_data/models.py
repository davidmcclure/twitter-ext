

import re

from collections import namedtuple
from pyspark.sql import SparkSession, types as T

from .sources import GnipTweet


class ModelMeta(type):

    def __new__(meta, name, bases, dct):
        """Generate a namedtuple from the `schema` class attribute.
        """
        if isinstance(dct.get('schema'), T.StructType):

            Row = namedtuple(name, dct['schema'].names)

            # By default, default all fields to None.
            Row.__new__.__defaults__ = (None,) * len(Row._fields)

            bases = (Row,) + bases

        return super().__new__(meta, name, bases, dct)


class Model(metaclass=ModelMeta):

    @classmethod
    def from_rdd(cls, row):
        """Wrap a raw `Row` instance from an RDD as a model instance.

        Args:
            row (pyspark.sql.Row)

        Returns: Model
        """
        return cls(**row.asDict())


class Tweet(Model):

    schema = T.StructType([
        T.StructField('id', T.StringType(), nullable=False),
        T.StructField('body', T.StringType()),
        T.StructField('posted_time', T.TimestampType()),
        T.StructField('actor_id', T.StringType()),
        T.StructField('actor_display_name', T.StringType()),
        T.StructField('actor_summary', T.StringType()),
        T.StructField('actor_preferred_username', T.StringType()),
        T.StructField('actor_location', T.StringType()),
        T.StructField('actor_language', T.StringType()),
        T.StructField('location_display_name', T.StringType()),
        T.StructField('location_name', T.StringType()),
        T.StructField('location_country_code', T.StringType()),
        T.StructField('location_twitter_country_code', T.StringType()),
        T.StructField('location_twitter_place_type', T.StringType()),
        T.StructField('geo_lat', T.FloatType()),
        T.StructField('geo_lon', T.FloatType()),
    ])

    @classmethod
    def from_gnip_json(cls, json):
        """Make a row from raw Gnip JSON.
        """
        source = GnipTweet(json)

        return cls(**{
            name: getattr(source, name)()
            for name in cls.schema.names
        })

    def tokens(self):
        """Tokenize the tweet.
        """
        # Remove URLs.
        text = re.sub('http\S+', '', self.body)

        return re.findall('[a-z0-9#@]+', text.lower())
