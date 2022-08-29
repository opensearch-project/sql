#!/bin/bash

rm -rf opensearch-2.2.0/plugins/opensearch-sql-2.2.0.0-SNAPSHOT
unzip ../../plugin/build/distributions/opensearch-sql-2.2.0.0-SNAPSHOT.zip -d opensearch-2.2.0/plugins/opensearch-sql-2.2.0.0-SNAPSHOT
docker build --pull --rm -f "Dockerfile" -t os-ql:2.2.0 "."
