

import time

from twitter.utils import get_spark


def work(i):
    time.sleep(1)
    return i + 1


def main():
    sc, spark = get_spark()
    data = sc.parallelize(range(36))
    result = data.map(work).collect()
    print(result)


if __name__ == '__main__':
    main()
