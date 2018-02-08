

import pytest
import os

from twitter.jobs.load_tweets import main
from twitter.spark import spark


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


def test_load_tweets(tweets):
    print(tweets)
    print(tweets.count())
