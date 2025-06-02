#!/bin/bash
# Script to retrieve Sonatype credentials from AWS Secrets Manager

SONATYPE_USERNAME=$(aws secretsmanager get-secret-value --secret-id maven-snapshots-username --query SecretString --output text)
SONATYPE_PASSWORD=$(aws secretsmanager get-secret-value --secret-id maven-snapshots-password --query SecretString --output text)
echo "::add-mask::$SONATYPE_USERNAME"
echo "::add-mask::$SONATYPE_PASSWORD"
echo "SONATYPE_USERNAME=$SONATYPE_USERNAME" >> $GITHUB_ENV
echo "SONATYPE_PASSWORD=$SONATYPE_PASSWORD" >> $GITHUB_ENV