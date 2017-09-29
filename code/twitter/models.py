

from collections import namedtuple
from pyspark.sql import SparkSession, types as T


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
        T.StructField('text', T.StringType()),
        T.StructField('timestamp_ms', T.StringType()),
        T.StructField('lang', T.StringType()),
        T.StructField('source', T.StringType()),
        T.StructField('place', T.StringType()),

        T.StructField('user', T.StructType([
            T.StructField('screen_name', T.StringType()),
            T.StructField('name', T.StringType()),
            T.StructField('description', T.StringType()),
            T.StructField('location', T.StringType()),
            T.StructField('followers_count', T.IntegerType()),
            T.StructField('statuses_count', T.IntegerType()),
            T.StructField('url', T.StringType()),
            T.StructField('lang', T.StringType()),
        ])),

    ])

    @classmethod
    def from_api_json(cls, json):
        """Make a row from the raw API JSON.
        """
        return cls(

            id=json['id_str'],
            text=json['text'],
            timestamp_ms=json['timestamp_ms'],
            lang=json['lang'],
            source=json['source'],
            place=json['place'],

            user=dict(
                screen_name=json['user']['screen_name'],
                name=json['user']['name'],
                description=json['user']['description'],
                location=json['user']['location'],
                followers_count=json['user']['followers_count'],
                statuses_count=json['user']['statuses_count'],
                url=json['user']['url'],
                lang=json['user']['lang'],
            ),

        )


class CityTweet(Model):

    schema = T.StructType([
        T.StructField('city', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('text', T.StringType()),
    ])
