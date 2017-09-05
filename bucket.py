

import io
import re
import os

from services import s3


class Bucket:

    def __init__(self, bucket_name, prefix=''):
        """Connect to the bucket.

        Args:
            bucket_name (str)
            prefix (str)
        """
        self.bucket = s3.Bucket(bucket_name)
        self.prefix = prefix

    def scan(self, pattern=None):
        """List paths in the bucket, optionally under a prefix.

        Args:
            pattern (str)

        Yields: str
        """
        for obj in self.bucket.objects.filter(Prefix=self.prefix):
            if not pattern or re.search(pattern, obj.key):
                yield os.path.join('s3://', self.bucket.name, obj.key)
