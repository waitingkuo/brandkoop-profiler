#!/bin/bash



eval `docker-machine env dev`

docker build --no-cache=true -t waitingkuo/brandkoop-profiler .
#docker push waitingkuo/brandkoop-profiler

#docker `machine config brandkoop-dev` pull waitingkuo/brandkoop-profiler
# first time
# compose up -d
#docker-compose `machine config brandkoop-dev` restart


