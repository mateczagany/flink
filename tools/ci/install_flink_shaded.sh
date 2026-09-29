#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Installs an unreleased flink-shaded build into the local Maven repository used by CI, so that the
# Flink build that follows resolves every flink-shaded artifact (including
# flink-shaded-netty-tcnative-static, which the e2e tests need) from that build.
#
# Environment:
#   FLINK_SHADED_REF   git ref to build: branch, tag, full 40-character commit SHA or "pull/<n>/head".
#                      Unset or empty: nothing is built.
#   FLINK_SHADED_REPO  git repository URL, default https://github.com/apache/flink-shaded.git.
#                      Any fork works.
#   MAVEN_ARGS/PROFILE consumed via run_mvn (tools/ci/maven-utils.sh), so the artifacts land in the
#                      same -Dmaven.repo.local the Flink build uses.
#
# If the unreleased branch changed <flink.shaded.version> or the netty-tcnative version, pass the
# matching -Dflink.shaded.version=... / -Dflink.shaded.netty.tcnative.version=... in PROFILE.
#
# flink-shaded's master carries the *release* version (no -SNAPSHOT), so a locally installed build
# shares coordinates with the published one and would poison a CI Maven cache for later runs. Hence
# the local repository is marked before an unreleased install starts (so a failed or cancelled
# install is covered too), and the next run - with or without FLINK_SHADED_REF - removes the cached
# flink-shaded artifacts and the marker before building. Runs on an unmarked repository without
# FLINK_SHADED_REF touch nothing.

CI_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# maven-utils.sh must be sourced before enabling strict mode: set_mirror_config tests $? after a
# `curl | grep` pipeline (its fallback is unreachable under errexit) and it expands MAVEN_ARGS and
# PROFILE unguarded (aborts under nounset).
source "${CI_DIR}/maven-utils.sh"

# run_mvn reads this unguarded; default it so the script's nounset does not abort inside run_mvn.
export MVN_RUN_VERBOSE="${MVN_RUN_VERBOSE:-}"

set -o errexit
set -o nounset
set -o pipefail

FLINK_SHADED_REPO="${FLINK_SHADED_REPO:-https://github.com/apache/flink-shaded.git}"
FLINK_SHADED_REF="${FLINK_SHADED_REF:-}"

# -Dmaven.repo.local=<dir> is the only thing in MAVEN_ARGS; fall back to the default location.
local_repo=$(echo "${MAVEN_ARGS:-}" | sed -n 's/.*-Dmaven.repo.local=\([^ ]*\).*/\1/p')
local_repo="${local_repo:-$HOME/.m2/repository}"

marker="${local_repo}/.flink-shaded-unreleased"
if [ -n "${FLINK_SHADED_REF}" ] || [ -f "${marker}" ]; then
    echo "Removing cached flink-shaded artifacts from ${local_repo}"
    find "${local_repo}/org/apache/flink" -maxdepth 1 -type d -name 'flink-shaded*' -exec rm -rf {} + 2>/dev/null || true
    rm -f "${marker}"
fi

if [ -z "${FLINK_SHADED_REF}" ]; then
    echo "FLINK_SHADED_REF not set; using released flink-shaded artifacts."
    exit 0
fi

clone_dir=$(mktemp -d)
trap 'rm -rf "${clone_dir}"' EXIT

echo "Fetching flink-shaded ${FLINK_SHADED_REF} from ${FLINK_SHADED_REPO}"
git -C "${clone_dir}" init --quiet
# fetch by ref instead of clone+checkout so that branches, tags, commit SHAs and pull/<n>/head all work
git -C "${clone_dir}" fetch --depth 1 "${FLINK_SHADED_REPO}" "${FLINK_SHADED_REF}"
git -C "${clone_dir}" checkout --quiet FETCH_HEAD
echo "flink-shaded commit: $(git -C "${clone_dir}" rev-parse HEAD)"

# MVN_RUN_VERBOSE=false: run_mvn would otherwise print `mvn --version` and an "Invoking mvn" line
# into the captured output.
installed_version=$(MVN_RUN_VERBOSE=false run_mvn -q -f "${clone_dir}/pom.xml" org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -DforceStdout)
echo "Installing flink-shaded ${installed_version} (with netty-tcnative-static) into ${local_repo}"
mkdir -p "${local_repo}"
touch "${marker}"
# PROFILE carries Flink-specific profiles (e.g. -Pjava17-target); Maven only warns that they do not
# exist in flink-shaded, so run_mvn can be reused for the mirror and repo.local settings.
run_mvn -f "${clone_dir}/pom.xml" clean install -DskipTests -Pinclude-netty-tcnative-static

flink_shaded_version=$(cd "${CI_DIR}/../.." && MVN_RUN_VERBOSE=false run_mvn -q -N org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=flink.shaded.version -DforceStdout)
if [ "${installed_version}" != "${flink_shaded_version}" ]; then
    echo "WARNING: installed flink-shaded ${installed_version} but Flink resolves flink.shaded.version=${flink_shaded_version}."
    echo "         Add -Dflink.shaded.version=${installed_version} to PROFILE, otherwise the released version is used."
fi
