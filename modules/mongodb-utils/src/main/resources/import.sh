#!/usr/bin/env bash
cd "$(dirname "$0")"

export HADOOP_CLASSPATH=`hadoop classpath`:./mongo-hadoop-core-1.5.2.jar:./mongo-java-driver-2.14.3.jar
yarn jar ./infoworks-mongodb-utils-1.0-SNAPSHOT.jar io.infoworks.datalake.ingestion.MongoDbMRJob -libjars ./mongo-hadoop-core-1.5.2.jar,./mongo-java-driver-2.14.3.jar $@
