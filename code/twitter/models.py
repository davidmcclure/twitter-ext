

import iso8601

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

            bases = (Row,)

        return super().__new__(meta, name, bases, dct)


class Model(metaclass=ModelMeta):
    pass


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

            id=source['id'],
            body=source['body'],
            posted_time=source.posted_time(),

            actor=dict(
                id=source['actor', 'id'],
                display_name=source['actor', 'displayName'],
                summary=source['actor', 'summary'],
                preferred_username=source['actor', 'preferredUsername'],
                location=source['actor', 'location', 'displayName'],
            ),

            location=dict(
                display_name=source['location', 'displayName'],
                name=source['location', 'name'],
                country_code=source['location', 'countryCode'],
                twitter_country_code=source['location', 'twitterCountryCode'],
                twitter_place_type=source['location', 'twitterPlaceType'],
            ),

            geo=dict(
                lat=source.lat(),
                lon=source.lon(),
            ),

        )


class CityTweet(Model):

    schema = T.StructType([
        T.StructField('city', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('text', T.StringType()),
    ])


class StateTweet(Model):

    schema = T.StructType([
        T.StructField('state', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('text', T.StringType()),
    ])
