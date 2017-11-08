
# Twitter + Spark

A feature extraction harness for Twitter data, designed for projects at the [Lab for Social Machines](http://socialmachines.media.mit.edu/).

## Build a development environment

Though in the future it might make sense to abstract this out into a fully generic template, for now this assumes that you're working with a project-specific fork of this repository.

1. Download and install [Docker](https://www.docker.com/docker-mac).

1. Under `/build`, rename `dev.env.changeme` to `dev.env`. This file can contain arbitrary ENV variables for the local development environment - AWS credentials, etc. `dev.env` is ignored by git.

1. In `docker-compose.yml`, set the `image` key to a name with format `<username>/<project name>`.

1. Build the Docker image with: `docker-compose build`. On the first run, this will take 5-10 minutes, depending on internet speed.

1. Once the build finishes, run the container locally and attach a bash shell with: `docker-compose run dev bash`.

In the container, you'll get a fully configured installation of Java 8, Spark 2.2, Python 3.6, and the project code. Some things to note:

- The `./code` directory (with the Python source) is mounted as a linked volume to `/code` in the container. Changes made to `./code` on the host filesystem will get automatically synced into the container, which makes it possible to edit code and do version control in the host OS. Just leave the container running in a tab, and switch there when you need to run code.

- The `./data` directory is mounted to `/data` in the container. This makes it easy to add testing data sources for use in the container.

- All of the Spark executables are available globally. Eg, to get a Spark shell, just run `pyspark`, which will automatically run under the IPython interpreter installed via the project dependencies.

- Run jobs with `spark-submit twitter/jobs/<name>.py`. To pass custom arguments to the jobs, use `--` to mark the beginning of the arg string (an IPython [quirk](https://stackoverflow.com/a/22632197)), and then pass arguments in the normal way. Eg, if we had some Gnip data in `./data/gnip`, to ingest the Tweets:

  `spark-submit twitter/jobs/load_tweets.py -- --src /data/gnip`

## Deploy standalone Spark cluster to EC2

TODO
