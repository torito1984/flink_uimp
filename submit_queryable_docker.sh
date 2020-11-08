#!/bin/bash

JOB_CLASS_NAME="com.flink.iot.basics.queryable.QueryableCustomerServiceJob"
JM_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
docker cp ./target/scala-2.12/flink-exercises-scala-es-assembly-1.0.0.jar "${JM_CONTAINER}":/job.jar
docker exec -t -i "${JM_CONTAINER}" flink run -d -c ${JOB_CLASS_NAME} /job.jar