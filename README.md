
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

- The code abstracts over the differences between reading / writing data from the local filesystem versus S3. This makes it easy to test on small samples of data on the local disk, and then scale up to very large inputs without changing the code. Eg, you could also just pass in an S3 key (using the `s3a` protocol), and the code will automatically detect the S3 path and act accordingly.

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

Once the image is built locally, it can be deployed as a standalone Spark cluster on EC2. First we push the image to Docker hub to make it available EC2 nodes, and then use Ansible to automate the process of spinning up the cluster, pulling the images, and running the containers under the right configuration on the master / worker nodes.

### Publish image

1. If you don't already have one, create an account on [Docker Hub](https://hub.docker.com).

1. Create a new repository on Docker hub to house the image.

1. In `docker-compose.yml`, be sure that the `image` key matches the Hub user / repo name. Eg, if the user account is `lsm`, and the repo is called `twitter`, use `lsm/twitter` as the image name.

1. Update the image with `docker-compose build`. This will bake the current source code into the image.

1. Push the image to Docker Hub with `docker push <user>/<repo>`. The first time, this will take a couple minutes to push up the whole image, which is ~1g. Subsequent pushes will be much faster, though, when just the source code layer is changed. **Important**: before pushing to a public repo, be sure that there aren't any config secrets baked into the image. The easiest approach is to just put everything in the `build/dev.env` file, which is patched in via docker-compose to the local dev environment, but not baked into the image itself.

1. Open `deploy/roles/spark/tasks/start.yml` and update the `image` and `name` keys to match the image on Docker Hub. Eg:

    ```yaml
    - name: Start container
      docker_container:
        image: lsm/twitter
        name: twitter
        ...
    ```

### Configure AWS

1. Checkout submodules in the Ansible rig with `git submodule update --init`.

1. Change into `/deploy` and create a Python 2.7 virtualenv with:

    ```
    virtualenv -p python2 env
    . env/bin/activate
    ```

1. Create an AWS security group called `spark` with these ports:

    - 0 - 65535 open for nodes in the `spark` group. (Necessary because Spark uses random port allocation for master <-> worker communication.)
    - 22 (SSH) for Ansible.
    - 8080 for the Spark web console.

1. Create an AWS subnet and keypair, if necessary. In `deploy/roles/ec2-instance/defaults/main.yml`, update `ec2_keypair` and `ec2_subnet`.

1. Create a file at `~/.twitter-ext.txt` and enter a strong password for Ansible Vault on the first line (used to encrypt secrets in config files).

1. Next, we'll provide AWS credenials in an encrypted vault file. From `./deploy` (with the `ansible.cfg` file in the working directory) Use Ansible Vault to create the file:

    `ansible-vault create group_vars/all/aws.vault.yml`

  This will drop you into vim. We need to add two sets of AWS credentials - one for the cluster itself, which will be used by Ansible when creating / configuring the nodes, and another that will be used by the application codeSpark when running jobs.

    ```yaml
    aws_access_key_id_ansible: XXX
    aws_secret_access_key_ansible: XXX

    aws_access_key_id_spark: XXX
    aws_secret_access_key_spark: XXX
    ```

  This distinction makes it possible, for example, to run the cluster nodes on a personal account, but read/write data owned by a different account. If everything is consolidated in a single account, just use the same credentials for both.

1. On the local machine, run `aws configure` and provide credentials for the account that will house the cluster.

1. Confirm that Ansible can access AWS by running:

    `./ec2/ec2.py --refresh-cache`

  If this gives a JSON blob of AWS resources for the correct account, everything is good to go.

### Create the base Docker AMI

First, we need to build a base Docker AMI. In theory, we could probably use one of the existing ECS AMIs, but - this gives us full control over the host environment in case we ever need it, and only needs to be done once in the lifecycle of a project.

1. From `./deploy`, run:

    ```
    ansible-playbook docker-base.start.yml
    ./ec2/ec2.py --refresh-cache
    ansible-playbook docker-base.deploy.yml
    ```

1. In the AWS console, find the `docker-base` image that was created by Ansible. Right click on it, and then hit Image > Create Image.

1. Wait until the image finishes, and then copy the AMD ID into `deploy/roles/ec2-instance/defaults/main.yml`, under `ec2_image`. This tells Ansible to use this image as the base for all of the cluster nodes.

### Deploy a cluster

Now, we're ready to deploy a Spark cluster. All of the above steps only have to be done once. Going forward, this is all we'll need to do.

1. Check the instance type and count settings in `deploy/roles/ec2-instances/roles/worker/meta/main.yml`. I've been using c3.8xlarge's, which are a nice mix of performance (32 cores, 60g ram, 320g SSDs) and cost ($1.6/hour). Depending on the size of the job, I've been using anywhere from 2-10 workers. Keep in mind that, with 10 nodes, this will run $16/hour, which, if you forget to take down the cluster at the end, will add up quickly. The advantage to this whole system, though, is that deploying a cluster is very quick - about ~2-3 minutes - so it's reasonable to put up a cluster on a per-job basis. Eg, put up 20 nodes, run a job on 640 cores for 30 minutes, and then take it down immediately when it finishes, for a cost ~$10.

1. Start the nodes with `ansible-playbook cluster.start.yml`, refresh the Ansible cache with `./ec2/ec2.py --refresh-cache`.

1. Deploy with `ansible-playbook cluster.deploy.yml`. The first time this runs on fresh nodes, this will take 2-3 minutes, since the nodes have to pull the complete Docker image (~1g) from the Hub. If you run this again on the same cluster (eg, if you make a code change locally, push a fresh snapshot of the image, and want to update the code on the existing cluster), this will be very fast, just a few seconds.

1. Once the deploy finishes, copy the IP of the master node and open the Spark web UI at `<master ip>:8080` in a browser.

### Run a job

1. SSH into the master node with `ssh ubuntu@<master ip>`.

1. Start a tmux session with `tmux new -s spark`. This makes it possible to run long jobs without worrying about SSH timeouts.

1. List the running Docker containers with `sudo docker ps`. You should see one container, the Spark master.

1. Copy the id of the container, and attach a bash shell to it with `sudo docker exec -it <ID> bash`.

1. Now, run jobs just like in the local development envirnoment:

    `spark-submit twitter/jobs/load_tweets.py -- --src s3a://bucket/twitter --dest s3a://bucket/result.parquet`
