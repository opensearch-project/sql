#!/bin/bash
# Script to retrieve Sonatype credentials from AWS Secrets Manager

SONATYPE_USERNAME=op://opensearch-infra-secrets/maven-central-portal-credentials/username
SONATYPE_PASSWORD=op://opensearch-infra-secrets/maven-central-portal-credentials/password
echo "::add-mask::$SONATYPE_USERNAME"
echo "::add-mask::$SONATYPE_PASSWORD"
echo "SONATYPE_USERNAME=$SONATYPE_USERNAME" >> $GITHUB_ENV
echo "SONATYPE_PASSWORD=$SONATYPE_PASSWORD" >> $GITHUB_ENV