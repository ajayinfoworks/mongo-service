#!/usr/bin/env bash
cd "$(dirname "$0")"

nohup java -jar ./infoworks-mongodb-rest-server-1.0-SNAPSHOT.jar &
