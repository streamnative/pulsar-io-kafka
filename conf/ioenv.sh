#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
#

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

########################################
# default settings
########################################

# Configuration file of settings
# PULSAR_IO_CONF=

# Extra options to be passed to the jvm
# PULSAR_IO_EXTRA_OPTS=

# Add extra paths to the classpath
# PULSAR_IO_EXTRA_CLASSPATH=

# Folder where the PID file should be stored
# PULSAR_IO_PID_DIR=

#################################
# BookKeeper Logging Options
#################################

# Log4j configuration file
# PULSAR_IO_LOG_CONF=

# Logs location
# PULSAR_IO_LOG_DIR=

# Log file name
# PULSAR_IO_LOG_FILE="psegment.log"

# Log level & appender
# PULSAR_IO_ROOT_LOGGER="INFO,CONSOLE"

#################################
# JVM memory options
#################################

# PULSAR_IO_MAX_HEAP_MEMORY=1g
# PULSAR_IO_MIN_HEAP_MEMORY=1g
# PULSAR_IO_MAX_DIRECT_MEMORY=2g
# PULSAR_IO_MEM_OPTS=

# JVM GC options
# PULSAR_IO_GC_OPTS=

# JVM GC logging options
# PULSAR_IO_GC_LOGGING_OPTS=

# JVM performance options
# PULSAR_IO_PERF_OPTS="-XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch -XX:-UseBiasedLocking"
