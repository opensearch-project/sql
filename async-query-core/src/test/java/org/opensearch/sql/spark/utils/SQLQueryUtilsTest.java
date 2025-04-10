/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.spark.utils.SQLQueryUtilsTest.IndexQuery.index;
import static org.opensearch.sql.spark.utils.SQLQueryUtilsTest.IndexQuery.mv;
import static org.opensearch.sql.spark.utils.SQLQueryUtilsTest.IndexQuery.skippingIndex;

import java.util.List;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.flint.FlintIndexType;

@ExtendWith(MockitoExtension.class)
public class SQLQueryUtilsTest {

  @Mock private DataSource dataSource;

  @Test
  void testExtractionOfTableNameFromSQLQueries() {
    String sqlQuery = "select * from my_glue.default.http_logs";
    FullyQualifiedTableName fullyQualifiedTableName =
        SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName("my_glue", "default", "http_logs", fullyQualifiedTableName);

    sqlQuery = "select * from my_glue.db.http_logs";
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFullyQualifiedTableName("my_glue", "db", "http_logs", fullyQualifiedTableName);

    sqlQuery = "select * from my_glue.http_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName(null, "my_glue", "http_logs", fullyQualifiedTableName);

    sqlQuery = "select * from http_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName(null, null, "http_logs", fullyQualifiedTableName);

    sqlQuery = "DROP TABLE myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName("myS3", "default", "alb_logs", fullyQualifiedTableName);

    sqlQuery = "DESCRIBE TABLE myS3.default.alb_logs";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName("myS3", "default", "alb_logs", fullyQualifiedTableName);

    sqlQuery =
        "CREATE EXTERNAL TABLE\n"
            + "myS3.default.alb_logs\n"
            + "[ PARTITIONED BY (col_name [, … ] ) ]\n"
            + "[ ROW FORMAT DELIMITED row_format ]\n"
            + "STORED AS file_format\n"
            + "LOCATION { 's3://bucket/folder/' }";
    fullyQualifiedTableName = SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery).get(0);
    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName("myS3", "default", "alb_logs", fullyQualifiedTableName);
  }

  @Test
  void testMultipleTables() {
    String[] sqlQueries = {
      "SELECT * FROM my_glue.default.http_logs, my_glue.default.access_logs",
      "SELECT * FROM my_glue.default.http_logs LEFT JOIN my_glue.default.access_logs",
      "SELECT table1.id, table2.id FROM my_glue.default.http_logs table1 LEFT OUTER JOIN"
          + " (SELECT * FROM my_glue.default.access_logs) table2 ON table1.tag = table2.tag",
      "SELECT table1.id, table2.id FROM my_glue.default.http_logs FOR VERSION AS OF 1 table1"
          + " LEFT OUTER JOIN"
          + " (SELECT * FROM my_glue.default.access_logs) table2"
          + " ON table1.tag = table2.tag"
    };

    for (String sqlQuery : sqlQueries) {
      List<FullyQualifiedTableName> fullyQualifiedTableNames =
          SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery);

      assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
      assertEquals(2, fullyQualifiedTableNames.size());
      assertFullyQualifiedTableName(
          "my_glue", "default", "http_logs", fullyQualifiedTableNames.get(0));
      assertFullyQualifiedTableName(
          "my_glue", "default", "access_logs", fullyQualifiedTableNames.get(1));
    }
  }

  @Test
  void testMultipleTablesWithJoin() {
    String sqlQuery =
        "select * from my_glue.default.http_logs LEFT JOIN my_glue.default.access_logs";

    List<FullyQualifiedTableName> fullyQualifiedTableNames =
        SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery);

    assertFalse(SQLQueryUtils.isFlintExtensionQuery(sqlQuery));
    assertFullyQualifiedTableName(
        "my_glue", "default", "http_logs", fullyQualifiedTableNames.get(0));
    assertFullyQualifiedTableName(
        "my_glue", "default", "access_logs", fullyQualifiedTableNames.get(1));
  }

  @Test
  void testNoFullyQualifiedTableName() {
    String sqlQuery = "SHOW tables";

    List<FullyQualifiedTableName> fullyQualifiedTableNames =
        SQLQueryUtils.extractFullyQualifiedTableNames(sqlQuery);

    assertEquals(0, fullyQualifiedTableNames.size());
  }

  @Test
  void testExtractionFromFlintSkippingIndexQueries() {
    String[] createSkippingIndexQueries = {
      "CREATE SKIPPING INDEX ON myS3.default.alb_logs (l_orderkey VALUE_SET)",
      "CREATE SKIPPING INDEX IF NOT EXISTS"
          + " ON myS3.default.alb_logs (l_orderkey VALUE_SET) "
          + " WITH (auto_refresh = true)",
      "CREATE SKIPPING INDEX ON myS3.default.alb_logs(l_orderkey VALUE_SET)"
          + " WITH (auto_refresh = true)",
      "CREATE SKIPPING INDEX ON myS3.default.alb_logs(l_orderkey VALUE_SET) "
          + " WHERE elb_status_code = 500 "
          + " WITH (auto_refresh = true)",
      "DROP SKIPPING INDEX ON myS3.default.alb_logs",
      "ALTER SKIPPING INDEX ON myS3.default.alb_logs WITH (auto_refresh = false)",
      "VACUUM SKIPPING INDEX ON myS3.default.alb_logs",
    };

    for (String query : createSkippingIndexQueries) {
      assertTrue(SQLQueryUtils.isFlintExtensionQuery(query), "Failed query: " + query);

      IndexQueryDetails indexQueryDetails = SQLQueryUtils.extractIndexDetails(query);
      FullyQualifiedTableName fullyQualifiedTableName =
          indexQueryDetails.getFullyQualifiedTableName();

      assertNull(indexQueryDetails.getIndexName());
      assertFullyQualifiedTableName("myS3", "default", "alb_logs", fullyQualifiedTableName);
      assertEquals(
          "flint_mys3_default_alb_logs_skipping_index", indexQueryDetails.openSearchIndexName());
    }
  }

  @Test
  void testExtractionFromFlintCoveringIndexQueries() {
    String[] coveringIndexQueries = {
      "CREATE INDEX elb_and_requestUri ON myS3.default.alb_logs(l_orderkey, l_quantity)",
      "CREATE INDEX IF NOT EXISTS elb_and_requestUri "
          + " ON myS3.default.alb_logs(l_orderkey, l_quantity) "
          + " WITH (auto_refresh = true)",
      "CREATE INDEX elb_and_requestUri ON myS3.default.alb_logs(l_orderkey, l_quantity)"
          + " WITH (auto_refresh = true)",
      "CREATE INDEX elb_and_requestUri ON myS3.default.alb_logs(l_orderkey, l_quantity) "
          + " WHERE elb_status_code = 500 "
          + " WITH (auto_refresh = true)",
      "DROP INDEX elb_and_requestUri ON myS3.default.alb_logs",
      "VACUUM INDEX elb_and_requestUri ON myS3.default.alb_logs",
      "ALTER INDEX elb_and_requestUri ON myS3.default.alb_logs WITH (auto_refresh = false)"
    };

    for (String query : coveringIndexQueries) {
      assertTrue(SQLQueryUtils.isFlintExtensionQuery(query), "Failed query: " + query);

      IndexQueryDetails indexQueryDetails = SQLQueryUtils.extractIndexDetails(query);
      FullyQualifiedTableName fullyQualifiedTableName =
          indexQueryDetails.getFullyQualifiedTableName();

      assertEquals("elb_and_requestUri", indexQueryDetails.getIndexName());
      assertFullyQualifiedTableName("myS3", "default", "alb_logs", fullyQualifiedTableName);
      assertEquals(
          "flint_mys3_default_alb_logs_elb_and_requesturi_index",
          indexQueryDetails.openSearchIndexName());
    }
  }

  @Test
  void testExtractionFromCreateMVQuery() {
    String mvQuery = "select * from my_glue.default.logs";
    String query = "CREATE MATERIALIZED VIEW mv_1 AS " + mvQuery + " WITH (auto_refresh = true)";

    assertTrue(SQLQueryUtils.isFlintExtensionQuery(query));
    IndexQueryDetails indexQueryDetails = SQLQueryUtils.extractIndexDetails(query);
    assertNull(indexQueryDetails.getIndexName());
    assertNull(indexQueryDetails.getFullyQualifiedTableName());
    assertEquals(mvQuery, indexQueryDetails.getMvQuery());
    assertEquals("mv_1", indexQueryDetails.getMvName());
    assertEquals("flint_mv_1", indexQueryDetails.openSearchIndexName());
  }

  @Test
  void testExtractionFromFlintMVQuery() {
    String[] mvQueries = {
      "DROP MATERIALIZED VIEW mv_1", "ALTER MATERIALIZED VIEW mv_1 WITH (auto_refresh = false)",
    };

    for (String query : mvQueries) {
      assertTrue(SQLQueryUtils.isFlintExtensionQuery(query));

      IndexQueryDetails indexQueryDetails = SQLQueryUtils.extractIndexDetails(query);
      FullyQualifiedTableName fullyQualifiedTableName =
          indexQueryDetails.getFullyQualifiedTableName();

      assertNull(indexQueryDetails.getIndexName());
      assertNull(fullyQualifiedTableName);
      assertNull(indexQueryDetails.getMvQuery());
      assertEquals("mv_1", indexQueryDetails.getMvName());
      assertEquals("flint_mv_1", indexQueryDetails.openSearchIndexName());
    }
  }

  @Test
  void testDescSkippingIndex() {
    String descSkippingIndex = "DESC SKIPPING INDEX ON mys3.default.http_logs";

    assertTrue(SQLQueryUtils.isFlintExtensionQuery(descSkippingIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(descSkippingIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();

    assertNull(indexDetails.getIndexName());
    assertNotNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.SKIPPING, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.DESCRIBE, indexDetails.getIndexQueryActionType());
    assertEquals("flint_mys3_default_http_logs_skipping_index", indexDetails.openSearchIndexName());
  }

  @Test
  void testDescCoveringIndex() {
    String descCoveringIndex = "DESC INDEX cv1 ON mys3.default.http_logs";

    assertTrue(SQLQueryUtils.isFlintExtensionQuery(descCoveringIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(descCoveringIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();

    assertEquals("cv1", indexDetails.getIndexName());
    assertNotNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.COVERING, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.DESCRIBE, indexDetails.getIndexQueryActionType());
    assertEquals("flint_mys3_default_http_logs_cv1_index", indexDetails.openSearchIndexName());
  }

  @Test
  void testDescMaterializedView() {
    String descMv = "DESC MATERIALIZED VIEW mv1";

    assertTrue(SQLQueryUtils.isFlintExtensionQuery(descMv));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(descMv);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();

    assertNull(indexDetails.getIndexName());
    assertEquals("mv1", indexDetails.getMvName());
    assertNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.MATERIALIZED_VIEW, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.DESCRIBE, indexDetails.getIndexQueryActionType());
    assertEquals("flint_mv1", indexDetails.openSearchIndexName());
  }

  @Test
  void testShowIndex() {
    String showCoveringIndex = "SHOW INDEX ON myS3.default.http_logs";

    assertTrue(SQLQueryUtils.isFlintExtensionQuery(showCoveringIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(showCoveringIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();

    assertNull(indexDetails.getIndexName());
    assertNull(indexDetails.getMvName());
    assertNotNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.COVERING, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.SHOW, indexDetails.getIndexQueryActionType());
    assertNull(indexDetails.openSearchIndexName());
  }

  @Test
  void testShowMaterializedView() {
    String showMV = "SHOW MATERIALIZED VIEW IN my_glue.default";

    assertTrue(SQLQueryUtils.isFlintExtensionQuery(showMV));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(showMV);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();

    assertNull(indexDetails.getIndexName());
    assertNull(indexDetails.getMvName());
    assertNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.MATERIALIZED_VIEW, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.SHOW, indexDetails.getIndexQueryActionType());
    assertNull(indexDetails.openSearchIndexName());
  }

  @Test
  void testRefreshIndex() {
    String refreshSkippingIndex = "REFRESH SKIPPING INDEX ON mys3.default.http_logs";
    assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshSkippingIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(refreshSkippingIndex);
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    assertNull(indexDetails.getIndexName());
    assertNotNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.SKIPPING, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.REFRESH, indexDetails.getIndexQueryActionType());

    String refreshCoveringIndex = "REFRESH INDEX cv1 ON mys3.default.http_logs";
    assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshCoveringIndex));
    indexDetails = SQLQueryUtils.extractIndexDetails(refreshCoveringIndex);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    assertEquals("cv1", indexDetails.getIndexName());
    assertNotNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.COVERING, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.REFRESH, indexDetails.getIndexQueryActionType());

    String refreshMV = "REFRESH MATERIALIZED VIEW mv1";
    assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshMV));
    indexDetails = SQLQueryUtils.extractIndexDetails(refreshMV);
    fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    assertNull(indexDetails.getIndexName());
    assertEquals("mv1", indexDetails.getMvName());
    assertNull(fullyQualifiedTableName);
    assertEquals(FlintIndexType.MATERIALIZED_VIEW, indexDetails.getIndexType());
    assertEquals(IndexQueryActionType.REFRESH, indexDetails.getIndexQueryActionType());
  }

  /** https://github.com/opensearch-project/sql/issues/2206 */
  @Test
  void testAutoRefresh() {
    assertFalse(
        SQLQueryUtils.extractIndexDetails(skippingIndex().getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertFalse(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "false").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "true").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "true").withSemicolon().getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("\"auto_refresh\"", "true").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("\"auto_refresh\"", "true").withSemicolon().getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("\"auto_refresh\"", "\"true\"").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex()
                    .withProperty("\"auto_refresh\"", "\"true\"")
                    .withSemicolon()
                    .getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertFalse(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("auto_refresh", "1").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertFalse(
        SQLQueryUtils.extractIndexDetails(skippingIndex().withProperty("interval", "1").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertFalse(
        SQLQueryUtils.extractIndexDetails(
                skippingIndex().withProperty("\"\"", "\"true\"").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertFalse(
        SQLQueryUtils.extractIndexDetails(index().getQuery()).getFlintIndexOptions().autoRefresh());

    assertFalse(
        SQLQueryUtils.extractIndexDetails(index().withProperty("auto_refresh", "false").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(index().withProperty("auto_refresh", "true").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                index().withProperty("auto_refresh", "true").withSemicolon().getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(mv().withProperty("auto_refresh", "true").getQuery())
            .getFlintIndexOptions()
            .autoRefresh());

    assertTrue(
        SQLQueryUtils.extractIndexDetails(
                mv().withProperty("auto_refresh", "true").withSemicolon().getQuery())
            .getFlintIndexOptions()
            .autoRefresh());
  }

  @Test
  void testRecoverIndex() {
    String refreshSkippingIndex =
        "RECOVER INDEX JOB `flint_spark_catalog_default_test_skipping_index`";
    assertTrue(SQLQueryUtils.isFlintExtensionQuery(refreshSkippingIndex));
    IndexQueryDetails indexDetails = SQLQueryUtils.extractIndexDetails(refreshSkippingIndex);
    assertEquals(IndexQueryActionType.RECOVER, indexDetails.getIndexQueryActionType());
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
      return new IndexQuery("CREATE MATERIALIZED VIEW mv_1 AS select * from my_glue.default.logs");
    }

    public IndexQuery withProperty(String key, String value) {
      query = String.format("%s with (%s = %s)", query, key, value);
      return this;
    }

    public IndexQuery withSemicolon() {
      query += ";";
      return this;
    }
  }

  private void assertFullyQualifiedTableName(
      String expectedDatasourceName,
      String expectedSchemaName,
      String expectedTableName,
      FullyQualifiedTableName fullyQualifiedTableName) {
    assertEquals(expectedDatasourceName, fullyQualifiedTableName.getDatasourceName());
    assertEquals(expectedSchemaName, fullyQualifiedTableName.getSchemaName());
    assertEquals(expectedTableName, fullyQualifiedTableName.getTableName());
  }
}
