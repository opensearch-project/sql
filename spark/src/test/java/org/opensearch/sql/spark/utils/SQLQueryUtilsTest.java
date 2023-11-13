/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import static org.opensearch.sql.spark.utils.SQLQueryUtilsTest.IndexQuery.index;
import static org.opensearch.sql.spark.utils.SQLQueryUtilsTest.IndexQuery.mv;
import static org.opensearch.sql.spark.utils.SQLQueryUtilsTest.IndexQuery.skippingIndex;

import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.flint.FlintIndexType;

@ExtendWith(MockitoExtension.class)
public class SQLQueryUtilsTest {

  @Test
  void testExtractionOfTableNameFromSQLQueries() {
    String sqlQuery = "select * from my_glue.default.http_logs";
    FullyQualifiedTableName fullyQualifiedTableName =
        SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    Assertions.assertEquals("my_glue", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "select * from my_glue.db.http_logs";
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertEquals("my_glue", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("db", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "select * from my_glue.http_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    Assertions.assertEquals("my_glue", fullyQualifiedTableName.getSchemaName());
    Assertions.assertNull(fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "select * from http_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    Assertions.assertNull(fullyQualifiedTableName.getDatasourceName());
    Assertions.assertNull(fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("http_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "DROP TABLE myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    Assertions.assertEquals("myS3", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("alb_logs", fullyQualifiedTableName.getTableName());

    sqlQuery = "DESCRIBE TABLE myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableName(sqlQuery);
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
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
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
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
    Assertions.assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
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
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(createCoveredIndexQuery));
    IndexQueryDetails indexQueryDetails =
        SQLQueryUtils.extractIndexDetails(createCoveredIndexQuery);
    FullyQualifiedTableName fullyQualifiedTableName =
        indexQueryDetails.getFullyQualifiedTableName();
    Assertions.assertEquals("elb_and_requestUri", indexQueryDetails.getIndexName());
    Assertions.assertEquals("myS3", fullyQualifiedTableName.getDatasourceName());
    Assertions.assertEquals("default", fullyQualifiedTableName.getSchemaName());
    Assertions.assertEquals("alb_logs", fullyQualifiedTableName.getTableName());
  }

  @Test
  void testExtractionFromFlintMVQuery() {
    String createCoveredIndexQuery =
        "CREATE MATERIALIZED VIEW mv_1 AS query=select * from my_glue.default.logs WITH"
            + " (auto_refresh = true)";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(createCoveredIndexQuery));
    IndexQueryDetails indexQueryDetails =
        SQLQueryUtils.extractIndexDetails(createCoveredIndexQuery);
    FullyQualifiedTableName fullyQualifiedTableName =
        indexQueryDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexQueryDetails.getIndexName());
    Assertions.assertNull(fullyQualifiedTableName);
    Assertions.assertEquals("mv_1", indexQueryDetails.getMvName());
  }

  @Test
  void testDescIndex() {
    String descSkippingIndex = "DESC SKIPPING INDEX ON mys3.default.http_logs";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(descSkippingIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(descSkippingIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexDetails.getIndexName());
    Assertions.assertNotNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.SKIPPING, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.DESCRIBE, indexDetails.getIndexQueryActionType());

    String descCoveringIndex = "DESC INDEX cv1 ON mys3.default.http_logs";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(descCoveringIndex));
    indexDetails = SQLQueryUtils.extractIndexDetails(descCoveringIndex);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertEquals("cv1", indexDetails.getIndexName());
    Assertions.assertNotNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.COVERING, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.DESCRIBE, indexDetails.getIndexQueryActionType());

    String descMv = "DESC MATERIALIZED VIEW mv1";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(descMv));
    indexDetails = SQLQueryUtils.extractIndexDetails(descMv);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexDetails.getIndexName());
    Assertions.assertEquals("mv1", indexDetails.getMvName());
    Assertions.assertNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.MATERIALIZED_VIEW, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.DESCRIBE, indexDetails.getIndexQueryActionType());
  }

  @Test
  void testShowIndex() {
    String showCoveringIndex = " SHOW INDEX ON myS3.default.http_logs";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(showCoveringIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(showCoveringIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexDetails.getIndexName());
    Assertions.assertNull(indexDetails.getMvName());
    Assertions.assertNotNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.COVERING, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.SHOW, indexDetails.getIndexQueryActionType());

    String showMV = "SHOW MATERIALIZED VIEW IN my_glue.default";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(showMV));
    indexDetails = SQLQueryUtils.extractIndexDetails(showMV);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexDetails.getIndexName());
    Assertions.assertNull(indexDetails.getMvName());
    Assertions.assertNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.MATERIALIZED_VIEW, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.SHOW, indexDetails.getIndexQueryActionType());
  }

  @Test
  void testRefreshIndex() {
    String refreshSkippingIndex = "REFRESH SKIPPING INDEX ON mys3.default.http_logs";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshSkippingIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(refreshSkippingIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexDetails.getIndexName());
    Assertions.assertNotNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.SKIPPING, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.REFRESH, indexDetails.getIndexQueryActionType());

    String refreshCoveringIndex = "REFRESH INDEX cv1 ON mys3.default.http_logs";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshCoveringIndex));
    indexDetails = SQLQueryUtils.extractIndexDetails(refreshCoveringIndex);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertEquals("cv1", indexDetails.getIndexName());
    Assertions.assertNotNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.COVERING, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.REFRESH, indexDetails.getIndexQueryActionType());

    String refreshMV = "REFRESH MATERIALIZED VIEW mv1";
    Assertions.assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshMV));
    indexDetails = SQLQueryUtils.extractIndexDetails(refreshMV);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    Assertions.assertNull(indexDetails.getIndexName());
    Assertions.assertEquals("mv1", indexDetails.getMvName());
    Assertions.assertNull(fullyQualifiedTableName);
    Assertions.assertEquals(FlintIndexType.MATERIALIZED_VIEW, indexDetails.getIndexType());
    Assertions.assertEquals(IndexQueryActionType.REFRESH, indexDetails.getIndexQueryActionType());
  }

  /** https://github.com/opensearch-project/sql/issues/2206 */
  @Test
  void testAutoRefresh() {
    Assertions.assertFalse(
        SQLQueryUtils.extractIndexDetails(skippingIndex().getQuery()).isAutoRefresh());

    Assertions.assertFalse(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "false").getQuery())
            .isAutoRefresh());

    Assertions.assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "true").getQuery())
            .isAutoRefresh());

    Assertions.assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("\"auto_refresh\"", "true").getQuery())
            .isAutoRefresh());

    Assertions.assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("\"auto_refresh\"", "\"true\"").getQuery())
            .isAutoRefresh());

    Assertions.assertFalse(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "1").getQuery())
            .isAutoRefresh());

    Assertions.assertFalse(
        SQLQueryUtils.extractIndexDetails(skippingIndex().withProperty("interval", "1").getQuery())
            .isAutoRefresh());

    Assertions.assertFalse(SQLQueryUtils.extractIndexDetails(index().getQuery()).isAutoRefresh());

    Assertions.assertFalse(
        SQLQueryUtils.extractIndexDetails(index().withProperty("auto_refresh", "false").getQuery())
            .isAutoRefresh());

    Assertions.assertTrue(
        SQLQueryUtils.extractIndexDetails(index().withProperty("auto_refresh", "true").getQuery())
            .isAutoRefresh());

    Assertions.assertTrue(
        SQLQueryUtils.extractIndexDetails(mv().withProperty("auto_refresh", "true").getQuery())
            .isAutoRefresh());
  }

  @Getter
  protected static class IndexQuery {
    private String query;

    private IndexQuery(String query) {
      this.query = query;
    }

    public static IndexQuery skippingIndex() {
      return new IndexQuery(
          "CREATE SKIPPING INDEX ON myS3.default.alb_logs" + "(l_orderkey VALUE_SET)");
    }

    public static IndexQuery index() {
      return new IndexQuery(
          "CREATE INDEX elb_and_requestUri ON myS3.default.alb_logs(l_orderkey, " + "l_quantity)");
    }

    public static IndexQuery mv() {
      return new IndexQuery(
          "CREATE MATERIALIZED VIEW mv_1 AS query=select * from my_glue.default.logs");
    }

    public IndexQuery withProperty(String key, String value) {
      query = String.format("%s with (%s = %s)", query, key, value);
      return this;
    }
  }
}
