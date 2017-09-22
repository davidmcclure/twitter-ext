#!/usr/bin/env bash

docker tag twitter-dev:latest \
  574648240144.dkr.ecr.us-east-1.amazonaws.com/twitter-dev:latest

eval $(aws ecr get-login --no-include-email)

docker push 574648240144.dkr.ecr.us-east-1.amazonaws.com/twitter-dev
