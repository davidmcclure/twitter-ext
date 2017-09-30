

import iso8601
import attr

from functools import reduce


@attr.s
class NestedJSON:

    tree = attr.ib()

    def __getitem__(self, path):
        """Look up a nested key, or return None if it doesn't exist.

        Args:
            path (tuple)
        """
        if not isinstance(path, tuple):
            path = (path,)

        def vivify(tree, level):

            i, key = level

            val = tree.get(key)

            # If we're at the end of the path, return the value.
            if i == len(path)-1:
                return val

            # Otherwise return the next sub-tree.
            else:
                return val if type(val) is dict else dict()

        return reduce(vivify, enumerate(path), self.tree)


class GnipTweet(NestedJSON):

    def posted_time(self):
        return iso8601.parse_date(self['postedTime'])

    def lat(self):
        coords = self['geo', 'coordinates']
        return coords[0] if coords else None

    def lon(self):
        coords = self['geo', 'coordinates']
        return coords[1] if coords else None
