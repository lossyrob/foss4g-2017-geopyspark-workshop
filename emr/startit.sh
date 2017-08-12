#!/usr/bin/env bash

IMAGE=quay.io/geodocker/jupyter-geopyspark:foss4g-2017-workshop

YARN_RM=$(xmllint --xpath "//property[name='yarn.resourcemanager.hostname']/value/text()"  /etc/hadoop/conf/yarn-site.xml)

mkdir -p /home/hadoop/notebooks
chown -R hadoop:hadoop /home/hadoop/notebooks

DOCKER_ENV="-e USER=hadoop \
-e ZOOKEEPERS=$YARN_RM \
${ENV_VARS[@]} \
-v /home/hadoop/notebooks:/home/hadoop/notebooks:rw \
-v /etc/hadoop/conf:/yarn \
-v /etc/hadoop/conf:/etc/hadoop/conf \
-v /usr/lib/hadoop-hdfs/bin:/usr/lib/hadoop-hdfs/bin"

DOCKER_OPT="-d --net=host --restart=always --memory-swappiness=0"

sudo docker pull $IMAGE
sudo docker run $DOCKER_OPT --name=geopyspark $DOCKER_ENV $IMAGE
