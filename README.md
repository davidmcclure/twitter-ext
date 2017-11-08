
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

- The code abstracts over the differences between reading / writing data from the local filesystem versus S3. This makes it easy to test on small amounts of local data, and then scale up to very large inputs without changing the code. Eg, you could also just pass in an S3 URI (using the `s3a` protocol), and the code will automatically detect the S3 path and act accordingly.

  `spark-submit twitter/jobs/load_tweets.py -- --src s3a://bucket/twitter`

  To get this functionality with new jobs, use the `twitter.fs` module when listing / reading files.

  ```python
  from twitter import fs

  # List files.
  paths = fs.list('s3a://bucket/twitter')
  paths = fs.list('/local/dir')

  # Optionally, with a regex on the file name.
  paths = fs.list('s3a://bucket/twitter', '\.json$')

  # Read bytes.
  data = fs.list('s3a://bucket/twitter/05.json.gz')
  data = fs.list('/local/dir/05.json.gz')
  ```

## Deploy cluster to EC2

Once the image is built locally, it can be deployed as a standalone Spark cluster on EC2. First we push the image to Docker hub, to make it available EC2 nodes, and then use Ansible to automate the process of spinning up the cluster, pulling the images, and running the containers under the right configuration on the master / worker nodes.
