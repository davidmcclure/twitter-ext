

import io
import re
import os

from urllib.parse import urlparse
from services import s3


class Bucket:

    @classmethod
    def from_url(cls, url):
        """Make an instance from a S3 URL.
        """
        parsed = urlparse(url)

        return cls(parsed.netloc, parsed.path.lstrip('/'))

    def __init__(self, bucket_name, prefix=None):
        """Connect to the bucket.

        Args:
            bucket_name (str)
            prefix (str)
        """
        self.bucket = s3.Bucket(bucket_name)
        self.prefix = prefix

    def scan(self, pattern=None):
        """List URLs in the bucket, optionally under a prefix.

        Args:
            pattern (str)

        Yields: str
        """
        for obj in self.bucket.objects.filter(Prefix=self.prefix or ''):
            if not pattern or re.search(pattern, obj.key):
                yield os.path.join('s3://', self.bucket.name, obj.key)
