

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

        T.StructField('actor', T.StructType([
            T.StructField('id', T.StringType()),
            T.StructField('display_name', T.StringType()),
            T.StructField('summary', T.StringType()),
            T.StructField('preferred_username', T.StringType()),
            T.StructField('location', T.StringType()),
            T.StructField('language', T.StringType()),
        ])),

        T.StructField('location', T.StructType([
            T.StructField('display_name', T.StringType()),
            T.StructField('name', T.StringType()),
            T.StructField('country_code', T.StringType()),
            T.StructField('twitter_country_code', T.StringType()),
            T.StructField('twitter_place_type', T.StringType()),
        ])),

        T.StructField('geo', T.StructType([
            T.StructField('lat', T.FloatType()),
            T.StructField('lon', T.FloatType()),
        ])),

    ])

    @classmethod
    def from_gnip_json(cls, json):
        """Make a row from raw Gnip JSON.
        """
        source = GnipTweet(json)

        return cls(

            id=source.id(),
            body=source.body(),
            posted_time=source.posted_time(),

            actor=dict(
                id=source.actor_id(),
                display_name=source.actor_display_name(),
                summary=source.actor_summary(),
                preferred_username=source.actor_preferred_username(),
                location=source.actor_location(),
                language=source.actor_language(),
            ),

            location=dict(
                display_name=source.loc_display_name(),
                name=source.loc_name(),
                country_code=source.loc_country_code(),
                twitter_country_code=source.loc_twitter_country_code(),
                twitter_place_type=source.loc_twitter_place_type(),
            ),

            geo=dict(
                lat=source.geo_lat(),
                lon=source.geo_lon(),
            ),

        )

    def tokens(self):
        """Tokenize the tweet.
        """
        # Remove URLs.
        text = re.sub('http\S+', '', self.body)

        return re.findall('[a-z0-9#@]+', text.lower())


class GeoTweet(Model):

    schema = T.StructType([

        T.StructField('id', T.StringType()),
        T.StructField('posted_time', T.TimestampType()),
        T.StructField('actor_id', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('body', T.StringType()),
        T.StructField('twitter_lon', T.FloatType()),
        T.StructField('twitter_lat', T.FloatType()),

        T.StructField('state', T.StringType()),
        T.StructField('city_geonameid', T.IntegerType()),
        T.StructField('city_name', T.StringType()),
        T.StructField('population', T.IntegerType()),
        T.StructField('geonames_lon', T.FloatType()),
        T.StructField('geonames_lat', T.FloatType()),

    ])
