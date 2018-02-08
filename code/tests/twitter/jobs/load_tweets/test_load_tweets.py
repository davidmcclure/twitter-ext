

import pytest
import os

from twitter.jobs.load_tweets import main
from twitter.spark import spark

from tests import FIXTURES_ROOT
from tests.utils import read_yaml


cases = read_yaml(__file__, 'cases.yml')


src = os.path.join(FIXTURES_ROOT, 'twitter/decahose')
dest = '/tmp/tweets.parquet'


@pytest.fixture(scope='module')
def tweets():
    """Run job, read output DF.
    """
    main.callback(src, dest)
    return spark.read.parquet(dest)


@pytest.mark.parametrize('id,fields', cases.items())
def test_load_tweets(tweets, id, fields):
    """Check rows.
    """
    row = tweets.filter(tweets.id==id).head()

    for key in fields.keys():
        assert row[key] == fields[key]
