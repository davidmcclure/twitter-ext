#!/usr/bin/env bash

# https://raw.githubusercontent.com/aocenas/spark-docker-swarm/master/provision.sh

CLUSTER_PREFIX=twitter-dev
SPARK_IMAGE=gettyimages/spark:2.0.2-hadoop-2.7
NUM_WORKERS=3

DRIVER_OPTIONS="\
  --driver amazonec2 \
  --amazonec2-security-group spark \
  --amazonec2-vpc-id vpc-10757b75 \
  --amazonec2-zone e"

# Create master.

MASTER_OPTIONS="$DRIVER_OPTIONS \
  --amazonec2-instance-type m3.medium \
  --engine-label role=master"

MASTER_NAME=${CLUSTER_PREFIX}-master

docker-machine create $MASTER_OPTIONS $MASTER_NAME

# Get the master IP.

MASTER_IP=$(aws ec2 describe-instances | jq -r \
  ".Reservations[].Instances[] | \
  select(.KeyName==\"$MASTER_NAME\" and .State.Name==\"running\") | \
  .PrivateIpAddress")

# Init swarm master, get token.

docker-machine ssh $MASTER_NAME \
  sudo docker swarm init --advertise-addr $MASTER_IP

TOKEN=$(docker-machine ssh $MASTER_NAME \
  sudo docker swarm join-token worker -q)

# Create workers.

WORKER_OPTIONS="$DRIVER_OPTIONS \
  --amazonec2-instance-type m3.medium \
  --engine-label role=worker"

WORKER_NAME=${CLUSTER_PREFIX}-worker-

for ID in $(seq $NUM_WORKERS)
do
  (

    # Create worker.
    docker-machine create $WORKER_OPTIONS $WORKER_NAME$ID

    # Join to cluster.
    docker-machine ssh $WORKER_NAME$ID \
      sudo docker swarm join --token $TOKEN $MASTER_IP:2377

  ) &
done
wait

# Make network.

eval $(docker-machine env $MASTER_NAME)
docker network create --driver overlay spark-network

# Deploy images.

docker service create \
  --name master \
  --constraint engine.labels.role==master \
  --replicas 1 \
  --network spark-network \
  ${SPARK_IMAGE} \
  spark-class org.apache.spark.deploy.master.Master

docker service create \
  --name worker \
  --constraint engine.labels.role==worker \
  --replicas $NUM_WORKERS \
  --network spark-network \
  ${SPARK_IMAGE} \
  spark-class org.apache.spark.deploy.worker.Worker spark://master:7077
