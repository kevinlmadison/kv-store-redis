#!/usr/bin/env bash

if [[ $1 = "master" ]]; then
	ls src/* | entr -r cargo run
else
	ls src/* | entr -r cargo run -- --port 6380 --replicaof 127.0.0.1 6379
fi
