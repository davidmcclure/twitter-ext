

import click

from twitter.utils import get_spark, clean_tweet


states = [
    'WI',
    'MN',
    'IA',
    'MI',
    'IL',
    'IN',
    'OH',
    'MO',
    'KS',
    'NE',
    'SD',
    'ND',
]


@click.command()
@click.option('--src', default='s3a://twitter-dev/geo-tweets2.parquet')
@click.option('--cities_dest', default='s3a://twitter-dev/midwest/cities.csv')
@click.option('--rural_dest', default='s3a://twitter-dev/midwest/rural.txt')
@click.option('--urban_dest', default='s3a://twitter-dev/midwest/urban.txt')
def main(src, cities_dest, rural_dest):
    """Dump midwest urban + rural corpora.
    """
    sc, spark = get_spark()

    tweets = spark.read.parquet(src)

    midwest = tweets.filter(tweets.state.isin(states))

    counts = (midwest
        .groupby(midwest.city_geonameid)
        .agg(countDistinct('id').alias('count')))

    cities = (midwest
        .select('city_geonameid', 'city_name', 'state', 'population')
        .distinct()
        .orderBy('population', ascending=False)
        .join(counts, 'city_geonameid'))

    rural = (midwest
        .filter(midwest.population < 100000)
        .rdd.map(lambda t: clean_tweet(t.body))
        .toDF()
        .coalesce(100))

    urban = (midwest
        .filter(midwest.population > 100000)
        .filter(midwest.population < 1000000)
        .rdd.map(lambda t: clean_tweet(t.body))
        .toDF()
        .coalesce(100))

    cities.write.mode('overwrite').csv(cities_dest)
    rural.write.mode('overwrite').text(rural_dest)
    urban.write.mode('overwrite').text(ruban_dest)


if __name__ == '__main__':
    main()
