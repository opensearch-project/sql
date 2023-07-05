/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar FlintSparkSqlExtensions;

import SparkSqlBase;


// Flint SQL Syntax Extension

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : skippingIndexStatement
    ;

skippingIndexStatement
    : describeSkippingIndexStatement
    | dropSkippingIndexStatement
    ;

describeSkippingIndexStatement
    : (DESC | DESCRIBE) SKIPPING INDEX ON tableName=multipartIdentifier
    ;

dropSkippingIndexStatement
    : DROP SKIPPING INDEX ON tableName=multipartIdentifier
    ;
