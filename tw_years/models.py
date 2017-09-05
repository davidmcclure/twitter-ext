

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
        T.StructField('text', T.StringType(), nullable=False),
        T.StructField('timestamp_ms', T.StringType(), nullable=False),
        T.StructField('lang', T.StringType(), nullable=False),
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
        )
