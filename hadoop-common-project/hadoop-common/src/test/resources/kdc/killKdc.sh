#!/bin/sh
ps -ef | grep apacheds | grep -v grep | awk '{printf $2"\n"}' | xargs -t --no-run-if-empty kill -9

