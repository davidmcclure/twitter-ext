

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
        T.StructField('timestamp_ms', T.IntegerType(), nullable=False),
        T.StructField('lang', T.StringType(), nullable=False),
    ])
