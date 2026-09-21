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

# Installs the runtime prerequisites of flink-shaded-netty-tcnative-dynamic on an e2e runner.
#
# The dynamically linked netty-tcnative native that Flink ships in opt/ loads the system's
# libssl.so.3 / libcrypto.so.3 and libapr-1.so.0 at runtime. Since netty-tcnative 2.0.76 (Flink
# uses 2.0.81 via flink-shaded 22.0) the Linux native is linked against the OPENSSL_3.2.0 symbol
# version, so an OpenSSL >= 3.2 is required. The ubuntu-24.04 CI runners only ship OpenSSL
# 3.0.x, which makes dlopen fail with "version `OPENSSL_3.2.0' not found".
#
# This script does the same as the apache/flink-ci-docker image (base/Dockerfile, commit
# d3e28ec7875104e9ee3eec213abaa02de0edd14c) that runs the unit tests: it extracts libssl.so.3 and
# libcrypto.so.3 from Ubuntu's libssl3t64 3.4.x package into /usr/local/lib, which ldconfig
# prefers over /usr/lib/x86_64-linux-gnu.
#
# Afterwards it extracts the shipped Linux native from the tcnative-dynamic jar and checks
# with ldd that all its dependencies and symbol versions resolve on this machine, so that a
# broken environment fails this CI step instead of the e2e test itself.
#
# Usage: install_openssl_for_tcnative.sh [<path to flink-shaded-netty-tcnative-dynamic jar>]
#   Without argument the jar is looked up in ${FLINK_DIR:-./build-target}/opt/.

set -Eeuo pipefail

# Ubuntu package providing OpenSSL 3.4.x; the same one the flink-ci-docker image uses.
LIBSSL_DEB="${LIBSSL_DEB:-libssl3t64_3.4.1-1ubuntu3_amd64.deb}"
LIBSSL_DEB_URL="http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/${LIBSSL_DEB}"
LIBSSL_DEB_SHA256="${LIBSSL_DEB_SHA256:-e7955dc73771755405b2bb00678830dba60b6c1af388a43273559677e51cea23}"

# The target directory must be on the default ld.so search path (/usr/local/lib is listed in
# /etc/ld.so.conf.d/libc.conf on Ubuntu and takes precedence over /usr/lib/x86_64-linux-gnu).
LIBSSL_INSTALL_DIR="/usr/local/lib"
# Symbol version the tcnative native links against; used to decide whether an install is necessary.
REQUIRED_SYMBOL_VERSION="OPENSSL_3.2.0"

TCNATIVE_JAR="${1:-}"
TCNATIVE_NATIVE="META-INF/native/liborg_apache_flink_shaded_netty4_netty_tcnative_linux_x86_64.so"

SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
  SUDO="sudo"
fi

function log {
  echo "[install_openssl_for_tcnative] $*"
}

function system_libssl {
  ldconfig -p | awk '/libssl\.so\.3 \(libc6,x86-64\)/ { print $NF; exit }'
}

function libssl_satisfies_requirement {
  local lib
  lib="$(system_libssl)"
  [[ -n "${lib}" ]] && grep -aq "${REQUIRED_SYMBOL_VERSION}" "${lib}"
}

function install_libssl_from_ubuntu_package {
  local work_dir
  work_dir="$(mktemp -d)"

  log "Downloading ${LIBSSL_DEB_URL}"
  curl -sSL --retry 5 --retry-delay 5 -o "${work_dir}/${LIBSSL_DEB}" "${LIBSSL_DEB_URL}"
  echo "${LIBSSL_DEB_SHA256}  ${work_dir}/${LIBSSL_DEB}" | sha256sum -c -

  log "Extracting libssl.so.3 and libcrypto.so.3 from ${LIBSSL_DEB} into ${LIBSSL_INSTALL_DIR}"
  dpkg-deb -x "${work_dir}/${LIBSSL_DEB}" "${work_dir}/extracted"
  ${SUDO} cp -a "${work_dir}/extracted/usr/lib/x86_64-linux-gnu/libssl.so.3" \
    "${work_dir}/extracted/usr/lib/x86_64-linux-gnu/libcrypto.so.3" "${LIBSSL_INSTALL_DIR}/"
  ${SUDO} ldconfig

  rm -rf "${work_dir}"
}

function verify_tcnative_native_loads {
  local jar="$1"
  local work_dir
  work_dir="$(mktemp -d)"

  log "Verifying that the native of ${jar} loads on this machine"
  unzip -q -o "${jar}" "${TCNATIVE_NATIVE}" -d "${work_dir}"

  local ldd_output
  ldd_output="$(ldd "${work_dir}/${TCNATIVE_NATIVE}" 2>&1 || true)"
  echo "${ldd_output}"
  if echo "${ldd_output}" | grep -q "not found"; then
    log "ERROR: the netty-tcnative native has unresolved dependencies or symbol versions on this machine (see ldd output above)."
    exit 1
  fi
  rm -rf "${work_dir}"
}

# libapr1 was renamed to libapr1t64 in Ubuntu 24.04's time_t transition
log "Installing packages required by netty-tcnative (libapr1t64) and by this script"
${SUDO} apt-get install -y bc libapr1t64 curl ca-certificates unzip

if libssl_satisfies_requirement; then
  log "System libssl $(system_libssl) already provides ${REQUIRED_SYMBOL_VERSION}; nothing to install"
else
  log "System libssl '$(system_libssl)' does not provide ${REQUIRED_SYMBOL_VERSION}"
  install_libssl_from_ubuntu_package
  if ! libssl_satisfies_requirement; then
    log "ERROR: ldconfig still resolves libssl.so.3 to '$(system_libssl)' which does not provide ${REQUIRED_SYMBOL_VERSION}"
    exit 1
  fi
fi

log "libssl.so.3 resolves to $(system_libssl)"
log "openssl CLI: $(command -v openssl) -> $(openssl version)"

if [[ -z "${TCNATIVE_JAR}" ]]; then
  TCNATIVE_JAR="$(ls "${FLINK_DIR:-./build-target}"/opt/flink-shaded-netty-tcnative-dynamic-*.jar 2> /dev/null | head -n 1 || true)"
fi
if [[ -z "${TCNATIVE_JAR}" || ! -f "${TCNATIVE_JAR}" ]]; then
  log "ERROR: no flink-shaded-netty-tcnative-dynamic jar found (looked in ${FLINK_DIR:-./build-target}/opt/). Pass the jar path as first argument."
  exit 1
fi
verify_tcnative_native_loads "${TCNATIVE_JAR}"
log "OpenSSL setup for netty-tcnative complete"
