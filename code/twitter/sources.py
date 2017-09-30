

import iso8601

from .utils import try_or_none


class GnipTweet(dict):

    def id(self):
        return self['id']

    def body(self):
        return self['body']

    def posted_time(self):
        return iso8601.parse_date(self['postedTime'])

    def actor_id(self):
        return self['actor']['id']

    def actor_display_name(self):
        return self['actor']['displayName']

    def actor_summary(self):
        return self['actor']['summary']

    def actor_preferred_username(self):
        return self['actor']['preferredUsername']

    def actor_language(self):
        return self['actor']['languages'][0]

    @try_or_none
    def actor_location(self):
        return self['actor']['location']['displayName']

    @try_or_none
    def loc_display_name(self):
        return self['location']['displayName']

    @try_or_none
    def loc_name(self):
        return self['location']['name']

    @try_or_none
    def loc_country_code(self):
        return self['location']['countryCode']

    @try_or_none
    def loc_twitter_country_code(self):
        return self['location']['twitterCountryCode']

    @try_or_none
    def loc_twitter_place_type(self):
        return self['location']['twitterPlaceType']

    @try_or_none
    def geo_lat(self):
        return float(self['geo']['coordinates'][0])

    @try_or_none
    def geo_lon(self):
        return float(self['geo']['coordinates'][1])
