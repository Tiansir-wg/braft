#!/bin/bash
sh stop.sh
cd ../../build
cmake .. && make
cd -
cmake . && make