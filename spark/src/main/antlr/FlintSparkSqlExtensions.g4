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
    | indexManagementStatement
    | indexJobManagementStatement
    ;

skippingIndexStatement
    : createSkippingIndexStatement
    | refreshSkippingIndexStatement
    | describeSkippingIndexStatement
    | alterSkippingIndexStatement
    | dropSkippingIndexStatement
    | vacuumSkippingIndexStatement
    | analyzeSkippingIndexStatement
    ;

createSkippingIndexStatement
    : CREATE SKIPPING INDEX (IF NOT EXISTS)?
        ON tableName
        LEFT_PAREN indexColTypeList RIGHT_PAREN
        whereClause?
        (WITH LEFT_PAREN propertyList RIGHT_PAREN)?
    ;

refreshSkippingIndexStatement
    : REFRESH SKIPPING INDEX ON tableName
    ;

describeSkippingIndexStatement
    : (DESC | DESCRIBE) SKIPPING INDEX ON tableName
    ;

alterSkippingIndexStatement
    : ALTER SKIPPING INDEX
        ON tableName
        WITH LEFT_PAREN propertyList RIGHT_PAREN
    ;

dropSkippingIndexStatement
    : DROP SKIPPING INDEX ON tableName
    ;

vacuumSkippingIndexStatement
    : VACUUM SKIPPING INDEX ON tableName
    ;

coveringIndexStatement
    : createCoveringIndexStatement
    | refreshCoveringIndexStatement
    | showCoveringIndexStatement
    | describeCoveringIndexStatement
    | alterCoveringIndexStatement
    | dropCoveringIndexStatement
    | vacuumCoveringIndexStatement
    ;

createCoveringIndexStatement
    : CREATE INDEX (IF NOT EXISTS)? indexName
        ON tableName
        LEFT_PAREN indexColumns=multipartIdentifierPropertyList RIGHT_PAREN
        whereClause?
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

alterCoveringIndexStatement
    : ALTER INDEX indexName
        ON tableName
        WITH LEFT_PAREN propertyList RIGHT_PAREN
    ;

dropCoveringIndexStatement
    : DROP INDEX indexName ON tableName
    ;

vacuumCoveringIndexStatement
    : VACUUM INDEX indexName ON tableName
    ;

analyzeSkippingIndexStatement
    : ANALYZE SKIPPING INDEX ON tableName
    ;

materializedViewStatement
    : createMaterializedViewStatement
    | refreshMaterializedViewStatement
    | showMaterializedViewStatement
    | describeMaterializedViewStatement
    | alterMaterializedViewStatement
    | dropMaterializedViewStatement
    | vacuumMaterializedViewStatement
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

alterMaterializedViewStatement
    : ALTER MATERIALIZED VIEW mvName=multipartIdentifier
        WITH LEFT_PAREN propertyList RIGHT_PAREN
    ;

dropMaterializedViewStatement
    : DROP MATERIALIZED VIEW mvName=multipartIdentifier
    ;

vacuumMaterializedViewStatement
    : VACUUM MATERIALIZED VIEW mvName=multipartIdentifier
    ;

indexManagementStatement
    : showFlintIndexStatement
    ;

showFlintIndexStatement
    : SHOW FLINT (INDEX | INDEXES) IN catalogDb=multipartIdentifier
    ;

indexJobManagementStatement
    : recoverIndexJobStatement
    ;

recoverIndexJobStatement
    : RECOVER INDEX JOB identifier
    ;

/*
 * Match all remaining tokens in non-greedy way
 * so WITH clause won't be captured by this rule.
 */
materializedViewQuery
    : .+?
    ;

whereClause
    : WHERE filterCondition
    ;

filterCondition
    : .+?
    ;

indexColTypeList
    : indexColType (COMMA indexColType)*
    ;

indexColType
    : identifier skipType=(PARTITION | VALUE_SET | MIN_MAX | BLOOM_FILTER)
        (LEFT_PAREN skipParams RIGHT_PAREN)?
    ;

skipParams
    : propertyValue (COMMA propertyValue)*
    ;

indexName
    : identifier
    ;

tableName
    : multipartIdentifier
    ;
