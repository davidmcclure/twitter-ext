

import os
import re
import scandir


def is_s3(path):
    """Is a path an S3 object path?
    """
    return path.startswith('s3://')


def scan(path, pattern=None):
    """Generate files that match a pattern.

    Args:
        path (str)
        pattern (str)
    """
    return (
        _scan_s3(path, pattern) if is_s3(path) else
        _scan_local(path, pattern)
    )


def _scan_local(path, pattern=None):
    """Scan local FS paths.
    """
    for root, dirs, files in scandir.walk(path, followlinks=True):
        for name in files:

            # Match the extension.
            if not pattern or re.search(pattern, name):
                yield os.path.join(root, name)


def _scan_s3(path, pattern=None):
    pass


def open(path):
    pass


def _open_local(path):
    pass


def _open_s3(path):
    pass
