#!/bin/bash
docker build -t milvus/celery .
docker tag milvus/celery registry.zilliz.com/milvus/celery:v0.0.1
docker push registry.zilliz.com/milvus/celery:v0.0.1

docker build -t celery_apps -f Dockerfile_apps .
docker tag celery_apps registry.zilliz.com/milvus/celery-apps:v0.0.1
docker push registry.zilliz.com/milvus/celery-apps:v0.0.1
