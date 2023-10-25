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
    | materializedViewStatement
    ;

skippingIndexStatement
    : createSkippingIndexStatement
    | refreshSkippingIndexStatement
    | describeSkippingIndexStatement
    | dropSkippingIndexStatement
    ;

createSkippingIndexStatement
    : CREATE SKIPPING INDEX (IF NOT EXISTS)?
        ON tableName
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
    : CREATE INDEX (IF NOT EXISTS)? indexName
        ON tableName
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

materializedViewStatement
    : createMaterializedViewStatement
    | refreshMaterializedViewStatement
    | showMaterializedViewStatement
    | describeMaterializedViewStatement
    | dropMaterializedViewStatement
    ;

createMaterializedViewStatement
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=multipartIdentifier
        AS query=materializedViewQuery
        (WITH LEFT_PAREN propertyList RIGHT_PAREN)?
    ;

refreshMaterializedViewStatement
    : REFRESH MATERIALIZED VIEW mvName=multipartIdentifier
    ;

showMaterializedViewStatement
    : SHOW MATERIALIZED (VIEW | VIEWS) IN catalogDb=multipartIdentifier
    ;

describeMaterializedViewStatement
    : (DESC | DESCRIBE) MATERIALIZED VIEW mvName=multipartIdentifier
    ;

dropMaterializedViewStatement
    : DROP MATERIALIZED VIEW mvName=multipartIdentifier
    ;

/*
 * Match all remaining tokens in non-greedy way
 * so WITH clause won't be captured by this rule.
 */
materializedViewQuery
    : .+?
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
