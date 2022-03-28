#!/bin/bash

set -e

function usage() {
    echo ""
    echo "This script is used to run Backwards Compatibility tests"
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo "None"
    echo ""
    echo -e "-h\tPrint this message."
    echo "--------------------------------------------------------------------------"
}

while getopts ":h" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${OPTARG}"
            exit 1
            ;;
    esac
done

# Place SQL artifact for the current version for bwc
function setup_bwc_artifact() {
    # This gets opensearch version from build.gradle (e.g. 1.2.0-SNAPSHOT),
    # then converts to plugin version by appending ".0" (e.g. 1.2.0.0-SNAPSHOT),
    # assuming one line in build.gradle is 'opensearch_version= System.getProperty("opensearch.version", "<opensearch_version>")'.
    plugin_version=$(grep 'opensearch_version = System.getProperty' build.gradle | \
        grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+[^"]*' | sed -e 's/\(.*\)\(\.[0-9]\)/\1\2.0/')
    plugin_artifact="./plugin/build/distributions/opensearch-sql-$plugin_version.zip"
    bwc_artifact_dir="./integ-test/src/test/resources/bwc/$plugin_version"

    if [ -z "${plugin_version// }" ]; then
        echo "Error: failed to retrieve plugin version from build.gradle." >&2
        exit 1
    fi

    # copy current artifact to bwc artifact directory if it's not there
    if [ ! -f "$bwc_artifact_dir/opensearch-sql-$plugin_version.zip" ]; then
        if [ ! -f "$plugin_artifact" ]; then
            ./gradlew assemble
        fi
        mkdir -p "$bwc_artifact_dir"
        cp "$plugin_artifact" "$bwc_artifact_dir"
    fi
}

setup_bwc_artifact
./gradlew bwcTestSuite -Dtests.security.manager=false

