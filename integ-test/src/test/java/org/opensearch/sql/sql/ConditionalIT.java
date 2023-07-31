/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.hitAny;
import static org.opensearch.sql.util.MatcherUtils.kvInt;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class ConditionalIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void ifnullShouldPassJDBC() throws IOException {
    JSONObject response = executeJdbcRequest(
        "SELECT IFNULL(lastname, 'unknown') AS name FROM " + TEST_INDEX_ACCOUNT
            + " GROUP BY name");
    assertEquals("IFNULL(lastname, 'unknown')", response.query("/schema/0/name"));
    assertEquals("name", response.query("/schema/0/alias"));
    assertEquals("keyword", response.query("/schema/0/type"));
  }

  @Test
  public void ifnullWithNullInputTest() {
    JSONObject response = new JSONObject(executeQuery(
            "SELECT IFNULL(null, firstname) as IFNULL1 ,"
                    + " IFNULL(firstname, null) as IFNULL2 ,"
                    + " IFNULL(null, null) as IFNULL3 "
                    + " FROM " + TEST_INDEX_BANK_WITH_NULL_VALUES
                    + " WHERE balance is null limit 2", "jdbc"));

    verifySchema(response,
            schema("IFNULL(null, firstname)", "IFNULL1", "keyword"),
            schema("IFNULL(firstname, null)", "IFNULL2", "keyword"),
            schema("IFNULL(null, null)", "IFNULL3", "byte"));
    verifyDataRows(response,
            rows("Hattie", "Hattie", LITERAL_NULL.value()),
            rows( "Elinor", "Elinor", LITERAL_NULL.value())
    );
  }

  @Test
  public void ifnullWithMissingInputTest() {
    JSONObject response = new JSONObject(executeQuery(
            "SELECT IFNULL(balance, 100) as IFNULL1, "
                    + " IFNULL(200, balance) as IFNULL2, "
                    + " IFNULL(balance, balance) as IFNULL3 "
                    + " FROM " + TEST_INDEX_BANK_WITH_NULL_VALUES
                    + " WHERE balance is null limit 3", "jdbc"));
    verifySchema(response,
            schema("IFNULL(balance, 100)", "IFNULL1", "long"),
            schema("IFNULL(200, balance)", "IFNULL2", "long"),
            schema("IFNULL(balance, balance)", "IFNULL3", "long"));
    verifyDataRows(response,
            rows(100, 200, null),
            rows(100, 200, null),
            rows(100, 200, null)
    );
  }

  @Test
  public void nullifShouldPassJDBC() throws IOException {
    JSONObject response = executeJdbcRequest(
            "SELECT NULLIF(lastname, 'unknown') AS name FROM " + TEST_INDEX_ACCOUNT);
    assertEquals("NULLIF(lastname, 'unknown')", response.query("/schema/0/name"));
    assertEquals("name", response.query("/schema/0/alias"));
    assertEquals("keyword", response.query("/schema/0/type"));
  }

  @Test
  public void nullifWithNotNullInputTestOne(){
    JSONObject response = new JSONObject(executeQuery(
            "SELECT NULLIF(firstname, 'Amber JOHnny') as testnullif "
                  + "FROM " + TEST_INDEX_BANK_WITH_NULL_VALUES
                  + " limit 2 ", "jdbc"));
    verifySchema(response,
            schema("NULLIF(firstname, 'Amber JOHnny')", "testnullif", "keyword"));
    verifyDataRows(response,
            rows(LITERAL_NULL.value()),
            rows("Hattie")
            );
  }

  @Test
  public void nullifWithNullInputTest() {
    JSONObject response = new JSONObject(executeQuery(
            "SELECT NULLIF(1/0, 123) as nullif1 ,"
                    + " NULLIF(123, 1/0) as nullif2 ,"
                    + " NULLIF(1/0, 1/0) as nullif3 "
                    + " FROM " + TEST_INDEX_BANK_WITH_NULL_VALUES
                    + " WHERE balance is null limit 1", "jdbc"));
    verifySchema(response,
            schema("NULLIF(1/0, 123)", "nullif1", "integer"),
            schema("NULLIF(123, 1/0)", "nullif2", "integer"),
            schema("NULLIF(1/0, 1/0)", "nullif3", "integer"));
    verifyDataRows(response,
            rows(LITERAL_NULL.value(), 123, LITERAL_NULL.value()
            )
    );
  }

  @Test
  public void isnullShouldPassJDBC() throws IOException {
    JSONObject response = executeJdbcRequest(
            "SELECT ISNULL(lastname) AS name FROM " + TEST_INDEX_ACCOUNT);
    assertEquals("ISNULL(lastname)", response.query("/schema/0/name"));
    assertEquals("name", response.query("/schema/0/alias"));
    assertEquals("boolean", response.query("/schema/0/type"));
  }

  @Test
  public void isnullWithNotNullInputTest() throws IOException {
    assertThat(
            executeQuery("SELECT ISNULL('elastic') AS isnull FROM " + TEST_INDEX_ACCOUNT),
            hitAny(kvInt("/fields/isnull/0", equalTo(0)))
    );
    assertThat(
            executeQuery("SELECT ISNULL('') AS isnull FROM " + TEST_INDEX_ACCOUNT),
            hitAny(kvInt("/fields/isnull/0", equalTo(0)))
    );
  }

  @Test
  public void isnullWithNullInputTest() {
    JSONObject response = new JSONObject(executeQuery(
            "SELECT ISNULL(1/0) as ISNULL1 ,"
                    + " ISNULL(firstname) as ISNULL2 "
                    + " FROM " + TEST_INDEX_BANK_WITH_NULL_VALUES
                    + " WHERE balance is null limit 2", "jdbc"));
    verifySchema(response,
            schema("ISNULL(1/0)", "ISNULL1", "boolean"),
            schema("ISNULL(firstname)", "ISNULL2", "boolean"));
    verifyDataRows(response,
            rows(LITERAL_TRUE.value(), LITERAL_FALSE.value()),
            rows(LITERAL_TRUE.value(), LITERAL_FALSE.value())
    );
  }

  @Test
  public void isnullWithMathExpr() throws IOException{
    assertThat(
            executeQuery("SELECT ISNULL(1+1) AS isnull FROM " + TEST_INDEX_ACCOUNT),
            hitAny(kvInt("/fields/isnull/0", equalTo(0)))
    );
    assertThat(
            executeQuery("SELECT ISNULL(1+1*1/0) AS isnull FROM " + TEST_INDEX_ACCOUNT),
            hitAny(kvInt("/fields/isnull/0", equalTo(1)))
    );
  }

  @Test
  public void ifShouldPassJDBC() throws IOException {
    JSONObject response = executeJdbcRequest(
        "SELECT IF(2 > 0, 'hello', 'world') AS name FROM " + TEST_INDEX_ACCOUNT);
    assertEquals("IF(2 > 0, 'hello', 'world')", response.query("/schema/0/name"));
    assertEquals("name", response.query("/schema/0/alias"));
    assertEquals("keyword", response.query("/schema/0/type"));
  }

  @Test
  public void ifWithTrueAndFalseCondition() throws IOException {
    JSONObject response = new JSONObject(executeQuery(
            "SELECT IF(2 < 0, firstname, lastname) as IF0, "
            + " IF(2 > 0, firstname, lastname) as IF1, "
            + " firstname as IF2, "
            + " lastname as IF3 "
            + " FROM " + TEST_INDEX_BANK_WITH_NULL_VALUES
            + " limit 2 ", "jdbc" ));
    verifySchema(response,
            schema("IF(2 < 0, firstname, lastname)", "IF0", "keyword"),
            schema("IF(2 > 0, firstname, lastname)", "IF1", "keyword"),
            schema("firstname", "IF2", "text"),
            schema("lastname", "IF3", "keyword")
            );
    verifyDataRows(response,
            rows("Duke Willmington", "Amber JOHnny", "Amber JOHnny", "Duke Willmington"),
            rows("Bond", "Hattie", "Hattie", "Bond")
    );

  }

  private SearchHits query(String query) throws IOException {
    final String rsp = executeQueryWithStringOutput(query);

    final XContentParser parser = new JsonXContentParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE,
        new JsonFactory().createParser(rsp));
    return SearchResponse.fromXContent(parser).getHits();
  }
}
