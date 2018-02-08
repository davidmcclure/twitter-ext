

import pytest
import os

from twitter.jobs.load_tweets import main
from twitter.spark import spark

from tests.utils import read_yaml


cases = read_yaml(__file__, 'cases.yml')


fixture_path = os.path.join(
    os.path.dirname(__file__),
    'fixtures/twitter/decahose',
)


@pytest.fixture(scope='module')
def tweets():
    """Run job, provide output DF.
    """
    main.callback(fixture_path, '/tmp/tweets.parquet')

    return spark.read.parquet('/tmp/tweets.parquet')


@pytest.mark.parametrize('id,fields', cases.items())
def test_load_tweets(tweets, id, fields):
    """Check rows.
    """
    row = tweets.filter(tweets.id==id).head()

    for key in fields.keys():
        assert row[key] == fields[key]
