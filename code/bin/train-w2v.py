

import os
import re
import click
import attr
import logging
import gensim

from glob import glob


logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
)


@attr.s
class LineCorpus:

    root = attr.ib()

    def paths(self):
        yield from glob(os.path.join(self.root, '*.txt'))

    def __iter__(self):
        for path in self.paths():
            with open(path) as fh:
                for line in fh:
                    yield re.findall('[#@\w]+', line.lower())


@click.command()
@click.argument('corpus_path', type=click.Path())
@click.argument('model_path', type=click.Path())
@click.option('--size', default=200)
@click.option('--min_count', default=1000)
@click.option('--workers', default=8)
def main(corpus_path, model_path, size, min_count, workers):

    sentences = LineCorpus(corpus_path)

    model = gensim.models.Word2Vec(
        sentences,
        size=size,
        min_count=min_count,
        workers=workers,
    )

    model.save(model_path)


if __name__ == '__main__':
    main()
