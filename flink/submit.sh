#!/bin/sh
# Builds the job jar and submits it to the Flink cluster from margo/docker-compose.yml.
# Needs sbt on JDK 17-21 (set JAVA_HOME) and the compose stack running.
set -e
cd "$(dirname "$0")"
sbt -batch assembly
docker cp target/scala-2.12/edith-impressions.jar margo-flink-jobmanager-1:/tmp/job.jar
docker exec margo-flink-jobmanager-1 flink run -d /tmp/job.jar
