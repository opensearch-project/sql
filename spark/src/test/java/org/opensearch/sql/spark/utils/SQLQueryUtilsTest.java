/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

@ExtendWith(MockitoExtension.class)
public class SQLQueryUtilsTest {

  @Test
  void testExtractionOfTableNameFromSQLQueries() {
    String sqlQuery = "select * from my_glue.default.http_logs";
    FullyQualifiedTableName fullyQualifiedTableName =
        SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertEquals("my_glue", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "select * from my_glue.db.http_logs";
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertEquals("my_glue", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("db", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "select * from my_glue.http_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertEquals("my_glue", fullyQualifiedTableName.getSchemaName());
    Assertions.assertNull(fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "select * from http_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertNull(fullyQualifiedTableName.getDatasourceName());
    Assertions.assertNull(fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "DROP TABLE myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertEquals("myS3", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("alb_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "DESCRIBE TABLE myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertEquals("myS3", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("alb_logs", fullyQualifiedTableName.getTableName());

    sqlQuery =
        "CREATE EXTERNAL TABLE\n"
            + "myS3.default.alb_logs\n"
            + "[ PARTITIONED BY (col_name [, … ] ) ]\n"
            + "[ ROW FORMAT DELIMITED row_format ]\n"
            + "STORED AS file_format\n"
            + "LOCATION { 's3://bucket/folder/' }";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertEquals("myS3", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("alb_logs", fullyQualifiedTableName.getTableName());
  }

  @Test
  void testErrorScenarios() {
    String sqlQuery = "SHOW tables";
    FullyQualifiedTableName fullyQualifiedTableName =
        SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertNotNull(fullyQualifiedTableName);
    Assertions.assertNull(fullyQualifiedTableName.getFullyQualifiedName());
    Assertions.assertNull(fullyQualifiedTableName.getSchemaName());
    Assertions.assertNull(fullyQualifiedTableName.getTableName());
    Assertions.assertNull(fullyQualifiedTableName.getDatasourceName());

    sqlQuery = "DESCRIBE TABLE FROM myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isIndexQuery(sqlQuery));
    Assertions.assertEquals("FROM", fullyQualifiedTableName.getFullyQualifiedName());
    Assertions.assertNull(fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("FROM", fullyQualifiedTableName.getTableName());
    Assertions.assertNull(fullyQualifiedTableName.getDatasourceName());
  }

  @Test
  void testExtractionFromFlintIndexQueries() {
    String createCoveredIndexQuery =
        "CREATE INDEX elb_and_requestUri ON myS3.default.alb_logs(l_orderkey, l_quantity) WITH"
            + " (auto_refresh = true)";
    Assertions.assertTrue(SQLQueryUtils.isIndexQuery(createCoveredIndexQuery));
    IndexDetails indexDetails = SQLQueryUtils.extractIndexDetails(createCoveredIndexQuery);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertEquals("elb_and_requestUri", indexDetails.getIndexName());
    Assertions.assertEquals("myS3", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("alb_logs", fullyQualifiedTableName.getTableName());
  }
}
