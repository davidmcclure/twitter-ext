

import click
import time

from twitter.utils import get_spark


def work(i):
    time.sleep(1)
    return i + 1


@click.command()
@click.argument('n', type=int)
def main(n):
    """Test parallelization.
    """
    sc, spark = get_spark()

    data = sc.parallelize(range(n))
    result = data.map(work).collect()

    print(result)


if __name__ == '__main__':
    main()
