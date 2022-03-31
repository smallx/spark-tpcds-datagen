#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Determine the current working directory
_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "${GENDATA_LOCATION}" ]; then
  echo "env GENDATA_LOCATION not defined" 1>&2
  exit 1
fi

${_DIR}/../bin/dsdgen \
--conf spark.master=yarn \
--conf spark.executor.instances=100 \
--conf spark.sql.shuffle.partitions=100 \
--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
--output-location ${GENDATA_LOCATION} \
--scale-factor 1000 \
--format parquet \
--partition-tables \
--cluster-by-partition-columns \
--num-partitions 100

echo "TPCDS data generated in ${GENDATA_LOCATION}"
