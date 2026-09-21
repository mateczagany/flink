#!/usr/bin/env bash

################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# NOTE: already sourced in common.sh

function _set_conf_ssl_helper {
    local type=$1 # 'internal' or external 'rest'
    local provider=$2 # 'JDK' or 'OPENSSL'
    local provider_lib=$3 # if using OPENSSL, choose: 'dynamic' or 'static' (how openSSL is linked to our packaged jar)
    local ssl_dir="${TEST_DATA_DIR}/ssl/${type}"
    local password="${type}.password"

    if [ "${type}" != "internal" ] && [ "${type}" != "rest" ]; then
        echo "Unknown type of ssl connectivity: ${type}. It can be either 'internal' or external 'rest'"
        exit 1
    fi
    if [ "${provider}" != "JDK" ] && [ "${provider}" != "OPENSSL" ]; then
        echo "Unknown SSL provider: ${provider}. It can be either 'JDK' or 'OPENSSL'"
        exit 1
    fi
    if [ "${provider_lib}" != "dynamic" ] && [ "${provider_lib}" != "static" ]; then
        echo "Unknown library type for openSSL: ${provider_lib}. It can be either 'dynamic' or 'static'"
        exit 1
    fi

    echo "Setting up SSL with: ${type} ${provider} ${provider_lib}"

    # clean up the dir that will be used for SSL certificates and trust stores
    if [ -e "${ssl_dir}" ]; then
       echo "File ${ssl_dir} exists. Deleting it..."
       rm -rf "${ssl_dir}"
    fi
    mkdir -p "${ssl_dir}"

    SANSTRING="dns:${NODENAME}"
    for NODEIP in $(get_node_ip) ; do
        SANSTRING="${SANSTRING},ip:${NODEIP}"
    done

    echo "Using SAN ${SANSTRING}"

    # create certificates
    keytool -genkeypair -alias ca -keystore "${ssl_dir}/ca.keystore" -dname "CN=Sample CA" -storepass ${password} -keypass ${password} -keyalg RSA -ext bc=ca:true -storetype PKCS12
    keytool -keystore "${ssl_dir}/ca.keystore" -storepass ${password} -alias ca -exportcert > "${ssl_dir}/ca.cer"
    keytool -importcert -keystore "${ssl_dir}/ca.truststore" -alias ca -storepass ${password} -noprompt -file "${ssl_dir}/ca.cer"

    keytool -genkeypair -alias node -keystore "${ssl_dir}/node.keystore" -dname "CN=${NODENAME}" -ext SAN=${SANSTRING} -storepass ${password} -keypass ${password} -keyalg RSA -storetype PKCS12
    keytool -certreq -keystore "${ssl_dir}/node.keystore" -storepass ${password} -alias node -file "${ssl_dir}/node.csr"
    keytool -gencert -keystore "${ssl_dir}/ca.keystore" -storepass ${password} -alias ca -ext SAN=${SANSTRING} -infile "${ssl_dir}/node.csr" -outfile "${ssl_dir}/node.cer"
    keytool -importcert -keystore "${ssl_dir}/node.keystore" -storepass ${password} -file "${ssl_dir}/ca.cer" -alias ca -noprompt
    keytool -importcert -keystore "${ssl_dir}/node.keystore" -storepass ${password} -file "${ssl_dir}/node.cer" -alias node -noprompt

    local additional_params
    additional_params=""
    if [[ ! "$(openssl version)" =~ OpenSSL\ 1 ]]; then
        # OpenSSL 3.x doesn't enable PKCS12 by default - we need to enable legacy algorithms
        additional_params="-legacy"
    fi

    # keystore is converted into a pem format to use it as node.pem with curl in Flink REST API queries, see also $CURL_SSL_ARGS
    openssl pkcs12 ${additional_params} -passin pass:${password} -in "${ssl_dir}/node.keystore" -out "${ssl_dir}/node.pem" -nodes

    if [ "${provider}" = "OPENSSL" -a "${provider_lib}" = "dynamic" ]; then
        # the dynamically linked tcnative loads the system's libssl/libcrypto (>= 3.2 required since
        # tcnative 2.0.76) and libapr-1; CI installs them via tools/ci/install_openssl_for_tcnative.sh
        cp $FLINK_DIR/opt/flink-shaded-netty-tcnative-dynamic-*.jar $FLINK_DIR/lib/
    elif [ "${provider}" = "OPENSSL" -a "${provider_lib}" = "static" ]; then
        # flink-shaded publishes the statically linked tcnative (bundling BoringSSL, which is Apache
        # licensed nowadays) to Maven Central; the Flink distribution itself only bundles the dynamic
        # variant in opt/. Fetch the static artifact with the same version as the bundled dynamic jar
        # instead of building it from a flink-shaded checkout during the test (FLINK-39002).
        local tcnative_version
        tcnative_version=$(ls $FLINK_DIR/opt/flink-shaded-netty-tcnative-dynamic-*.jar | sed -n 's/.*flink-shaded-netty-tcnative-dynamic-\(.*\)\.jar/\1/p')
        if [ -z "${tcnative_version}" ]; then
            echo "Could not determine the tcnative version from $FLINK_DIR/opt/flink-shaded-netty-tcnative-dynamic-*.jar"
            exit 1
        fi
        if [ ! -f "$FLINK_DIR/lib/flink-shaded-netty-tcnative-static-${tcnative_version}.jar" ]; then
            echo "Fetching org.apache.flink:flink-shaded-netty-tcnative-static:${tcnative_version} into $FLINK_DIR/lib/"
            retry_times_with_exponential_backoff 5 run_mvn -N -q --file "${END_TO_END_DIR}/pom.xml" \
                org.apache.maven.plugins:maven-dependency-plugin:copy \
                -Dartifact="org.apache.flink:flink-shaded-netty-tcnative-static:${tcnative_version}" \
                -DoutputDirectory="$FLINK_DIR/lib"
        fi
    fi

    if [ "${provider}" = "OPENSSL" ]; then
        verify_openssl_provider_available "${ssl_dir}"
    fi

    # adapt config
    set_config_key security.ssl.algorithms "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
    set_config_key security.ssl.provider ${provider}
    set_config_key security.ssl.${type}.enabled true
    set_config_key security.ssl.${type}.keystore ${ssl_dir}/node.keystore
    set_config_key security.ssl.${type}.keystore-password ${password}
    set_config_key security.ssl.${type}.key-password ${password}
    set_config_key security.ssl.${type}.truststore ${ssl_dir}/ca.truststore
    set_config_key security.ssl.${type}.truststore-password ${password}
}

# Fails fast if the netty OpenSSL provider cannot be loaded with the jars currently in $FLINK_DIR/lib,
# i.e. if OpenSsl.isAvailable() is false. Flink itself only detects this while creating its SSL
# contexts ("openSSL not available" in the JobManager/TaskManager logs), which surfaces in an e2e test
# as an opaque cluster start-up or handshake failure.
function verify_openssl_provider_available {
    local probe_dir=$1
    local java_cmd="java"
    if [ -n "${JAVA_HOME:-}" ]; then
        java_cmd="${JAVA_HOME}/bin/java"
    fi

    cat > "${probe_dir}/OpenSslProbe.java" <<'EOF'
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.OpenSsl;

public class OpenSslProbe {
    public static void main(String[] args) {
        if (OpenSsl.isAvailable()) {
            System.out.println("netty OpenSSL provider is available: " + OpenSsl.versionString());
        } else {
            System.err.println("netty OpenSSL provider is NOT available:");
            OpenSsl.unavailabilityCause().printStackTrace();
            System.exit(1);
        }
    }
}
EOF

    echo "Checking that the OPENSSL provider can be loaded from $FLINK_DIR/lib"
    if ! "${java_cmd}" -cp "$FLINK_DIR/lib/*" "${probe_dir}/OpenSslProbe.java"; then
        echo "[FAIL] security.ssl.provider=OPENSSL was requested but netty's OpenSsl.isAvailable() is false."
        echo "       Flink would fail with 'openSSL not available' when setting up SSL. Check that the"
        echo "       tcnative jar in $FLINK_DIR/lib ships a native for this platform and, for the dynamic"
        echo "       linkage, that libapr-1 and an OpenSSL >= 3.2 (OPENSSL_3.2.0) are installed."
        exit 1
    fi
}

function _set_conf_mutual_rest_ssl {
    local auth="${1:-server}" # only 'server' or 'mutual'
    local mutual="false"
    local ssl_dir="${TEST_DATA_DIR}/ssl/rest"
    if [ "${auth}" == "mutual" ]; then
        CURL_SSL_ARGS="${CURL_SSL_ARGS} --cert ${ssl_dir}/node.pem"
        mutual="true";
    fi
    echo "Mutual ssl auth: ${mutual}"
    set_config_key security.ssl.rest.authentication-enabled ${mutual}
}

function set_conf_rest_ssl {
    local auth="${1:-server}" # only 'server' or 'mutual'
    local provider="${2:-JDK}" # 'JDK' or 'OPENSSL'
    local provider_lib="${3:-dynamic}" # for OPENSSL: 'dynamic' or 'static'
    local ssl_dir="${TEST_DATA_DIR}/ssl/rest"
    _set_conf_ssl_helper "rest" "${provider}" "${provider_lib}"
    _set_conf_mutual_rest_ssl ${auth}
    REST_PROTOCOL="https"
    CURL_SSL_ARGS="${CURL_SSL_ARGS} --cacert ${ssl_dir}/node.pem"
}

function set_conf_ssl {
    local auth="${1:-server}" # only 'server' or 'mutual'
    local provider="${2:-JDK}" # 'JDK' or 'OPENSSL'
    local provider_lib="${3:-dynamic}" # for OPENSSL: 'dynamic' or 'static'
    _set_conf_ssl_helper "internal" "${provider}" "${provider_lib}"
    set_conf_rest_ssl ${auth} "${provider}" "${provider_lib}"
}

function rollback_openssl_lib() {
  rm -f $FLINK_DIR/lib/flink-shaded-netty-tcnative-{dynamic,static}-*.jar
}
