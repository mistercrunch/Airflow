#!/usr/bin/env bash
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

# This is an example docker build script. It is not intended for PRODUCTION use
set -euo pipefail
AIRFLOW_SOURCES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../" && pwd)"
cd "${AIRFLOW_SOURCES}"

# [START build]
export AIRFLOW_VERSION=2.2.2
export DEBIAN_VERSION="bullseye"

docker build . \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-${DEBIAN_VERSION}" \
    --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc" \
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless" \
    --tag "my-pypi-dev-runtime:0.0.1"
# [END build]
docker rmi --force "my-pypi-dev-runtime:0.0.1"
