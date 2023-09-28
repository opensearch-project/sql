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
    | coveringIndexStatement
    ;

skippingIndexStatement
    : createSkippingIndexStatement
    | refreshSkippingIndexStatement
    | describeSkippingIndexStatement
    | dropSkippingIndexStatement
    ;

createSkippingIndexStatement
    : CREATE SKIPPING INDEX ON tableName
        LEFT_PAREN indexColTypeList RIGHT_PAREN
        (WITH LEFT_PAREN propertyList RIGHT_PAREN)?
    ;

refreshSkippingIndexStatement
    : REFRESH SKIPPING INDEX ON tableName
    ;

describeSkippingIndexStatement
    : (DESC | DESCRIBE) SKIPPING INDEX ON tableName
    ;

dropSkippingIndexStatement
    : DROP SKIPPING INDEX ON tableName
    ;

coveringIndexStatement
    : createCoveringIndexStatement
    | refreshCoveringIndexStatement
    | showCoveringIndexStatement
    | describeCoveringIndexStatement
    | dropCoveringIndexStatement
    ;

createCoveringIndexStatement
    : CREATE INDEX indexName ON tableName
        LEFT_PAREN indexColumns=multipartIdentifierPropertyList RIGHT_PAREN
        (WITH LEFT_PAREN propertyList RIGHT_PAREN)?
    ;

refreshCoveringIndexStatement
    : REFRESH INDEX indexName ON tableName
    ;

showCoveringIndexStatement
    : SHOW (INDEX | INDEXES) ON tableName
    ;

describeCoveringIndexStatement
    : (DESC | DESCRIBE) INDEX indexName ON tableName
    ;

dropCoveringIndexStatement
    : DROP INDEX indexName ON tableName
    ;

indexColTypeList
    : indexColType (COMMA indexColType)*
    ;

indexColType
    : identifier skipType=(PARTITION | VALUE_SET | MIN_MAX)
    ;

indexName
    : identifier
    ;

tableName
    : multipartIdentifier
    ;
