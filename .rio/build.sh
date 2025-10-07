#!/bin/bash

set -exu  # Enable debugging and exit on error

# Install JDKs and set JAVA_HOME variables. We need all of them for BWC tests
for jdk in 11 17 21 24; do
    ci install-runtime "jdk-$jdk" --skip-runtime-symlink=false
    export "JAVA${jdk}_HOME"="$(readlink -f /app/.runtimes/jdk)"
done

curl -LO https://artifacts.apple.com/crypto-services-binaries-local/whisperctl/prod/${WHISPER_VERSION}/whisperctl_${WHISPER_VERSION}_linux_amd64.tar.gz
tar -xf whisperctl_${WHISPER_VERSION}_linux_amd64.tar.gz
mv whisperctl /usr/local/bin

export GRADLE_USER_HOME="${NON_ROOT_USER_HOME}/.gradle"
openssl x509 -in $RIO_NARRATIVE_CHAIN_PATH -text
whisperctl secret fetch \
  --output-dir "$GRADLE_USER_HOME" \
  --secret-name 'gradle.properties' \
  --namespace $WHISPER_NAMESPACE \
  --client-certificate-format PEM \
  --client-certificate $RIO_NARRATIVE_CHAIN_PATH \
  --client-key $RIO_NARRATIVE_PRIVATE_KEY_PATH

chown -R nonroot "${WORKSPACE}"
chown -R nonroot "${NON_ROOT_USER_HOME}"
find .cicd -exec chmod 0755 {} \;
chown -R root .cicd
chmod 0755 .

su nonroot -c "./gradlew \
--console=plain \
clean precommit test \
--init-script .rio/init-sdp.gradle \
--no-daemon -Dtests.haltonfailure=false"

chown -R root "${WORKSPACE}"
