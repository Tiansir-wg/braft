#!/bin/bash

# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# source shflags from current directory
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../shflags


# define command-line flags
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'
DEFINE_integer write_percentage 100 'Percentage of write operation'
DEFINE_integer bthread_concurrency '8' 'Number of worker pthreads'
DEFINE_integer server_port 8200 "Port of the first server"
DEFINE_integer server_num '3' 'Number of servers'
DEFINE_integer thread_num 1 'Number of sending thread'
DEFINE_string crash_on_fatal 'true' 'Crash on fatal log'
DEFINE_string log_each_request 'false' 'Print log for each request'
DEFINE_string valgrind 'false' 'Run in valgrind'
DEFINE_string use_bthread "true" "Use bthread to send request"

FLAGS "$@" || exit 1

# hostname prefers ipv6
IP=`hostname -i | awk '{print $NF}'`

if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
fi

raft_peers=""
for ((i=0; i<$FLAGS_server_num; ++i)); do
    raft_peers="${raft_peers}${IP}:$((${FLAGS_server_port}+i)):0,"
done

export TCMALLOC_SAMPLE_PARAMETER=524288

${VALGRIND} ./block_client \
        --write_percentage=${FLAGS_write_percentage} \
        --bthread_concurrency=${FLAGS_bthread_concurrency} \
        --conf="${raft_peers}" \
        --crash_on_fatal_log=${FLAGS_crash_on_fatal} \
        --log_each_request=${FLAGS_log_each_request} \
        --thread_num=${FLAGS_thread_num} \
        --use_bthread=${FLAGS_use_bthread} \
    
# gdbserver :2333 ${VALGRIND} ./block_client --write_percentage=100 --bthread_concurrency=8 --conf=192.168.108.2:8200:0,192.168.108.2:8201:0,192.168.108.2:8202:0, --crash_on_fatal_log=true --log_each_request=false --thread_num=1 --use_bthread=true

