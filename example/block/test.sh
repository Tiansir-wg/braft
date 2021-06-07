#!/bin/bash
sh run_server.sh  &
sleep 3
sh run_client.sh

sleep 30
sh stop.sh