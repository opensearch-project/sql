/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.util.Capability.EXPLAIN_FORMAT;
import static org.opensearch.sql.util.Capability.QUERY_ERROR_MESSAGE;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.RequiresCapability;

/** Test cases for writing missing join table aliases. */
public class JoinAliasWriterRuleIT extends SQLIntegTestCase {

  @Rule public final ExpectedException exception = ExpectedException.none();

  protected void init() throws Exception {
    loadIndex(Index.ORDER); // opensearch-sql_test_index_order
    loadIndex(Index.BANK); // opensearch-sql_test_index_bank
    loadIndex(Index.BANK_TWO); // opensearch-sql_test_index_bank_two
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void noTableAliasNoCommonColumns() throws IOException {
    sameExplain(
        query(
            "SELECT id, firstname",
            "FROM opensearch-sql_test_index_order",
            "INNER JOIN opensearch-sql_test_index_bank ",
            "ON name = firstname WHERE state = 'WA' OR id < 7"),
        query(
            "SELECT opensearch-sql_test_index_order_0.id,"
                + " opensearch-sql_test_index_bank_1.firstname ",
            "FROM opensearch-sql_test_index_order opensearch-sql_test_index_order_0 ",
            "INNER JOIN opensearch-sql_test_index_bank opensearch-sql_test_index_bank_1 ",
            "ON opensearch-sql_test_index_order_0.name = opensearch-sql_test_index_bank_1.firstname"
                + " ",
            "WHERE opensearch-sql_test_index_bank_1.state = 'WA' OR"
                + " opensearch-sql_test_index_order_0.id < 7"));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void oneTableAliasNoCommonColumns() throws IOException {
    sameExplain(
        query(
            "SELECT id, firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank ",
            "ON name = firstname WHERE state = 'WA' OR id < 7"),
        query(
            "SELECT a.id, opensearch-sql_test_index_bank_0.firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank opensearch-sql_test_index_bank_0 ",
            "ON a.name = opensearch-sql_test_index_bank_0.firstname ",
            "WHERE opensearch-sql_test_index_bank_0.state = 'WA' OR a.id < 7"));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void bothTableAliasNoCommonColumns() throws IOException {
    sameExplain(
        query(
            "SELECT id, firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON name = firstname WHERE state = 'WA' OR id < 7 "),
        query(
            "SELECT a.id, b.firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON a.name = b.firstname ",
            "WHERE b.state = 'WA' OR a.id < 7 "));
  }

  @Test
  @Ignore
  @RequiresCapability(EXPLAIN_FORMAT)
  public void tableNamesWithTypeName() throws IOException {
    sameExplain(
        query(
            "SELECT id, firstname ",
            "FROM opensearch-sql_test_index_order/_doc ",
            "INNER JOIN opensearch-sql_test_index_bank/account ",
            "ON name = firstname WHERE state = 'WA' OR id < 7"),
        query(
            "SELECT opensearch-sql_test_index_order_0.id,"
                + " opensearch-sql_test_index_bank_1.firstname ",
            "FROM opensearch-sql_test_index_order/_doc opensearch-sql_test_index_order_0 ",
            "INNER JOIN opensearch-sql_test_index_bank/_account opensearch-sql_test_index_bank_1 ",
            "ON opensearch-sql_test_index_order_0.name = opensearch-sql_test_index_bank_1.firstname"
                + " ",
            "WHERE opensearch-sql_test_index_bank_1.state = 'WA' OR"
                + " opensearch-sql_test_index_order_0.id < 7"));
  }

  @Ignore
  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void tableNamesWithTypeNameExplicitTableAlias() throws IOException {
    sameExplain(
        query(
            "SELECT id, firstname ",
            "FROM opensearch-sql_test_index_order/_doc a ",
            "INNER JOIN opensearch-sql_test_index_bank/account b ",
            "ON name = firstname WHERE state = 'WA' OR id < 7"),
        query(
            "SELECT a.id, b.firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON a.name = b.firstname ",
            "WHERE b.state = 'WA' OR a.id < 7"));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void actualTableNameAsAliasOnColumnFields() throws IOException {
    sameExplain(
        query(
            "SELECT opensearch-sql_test_index_order.id, b.firstname ",
            "FROM opensearch-sql_test_index_order ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON opensearch-sql_test_index_order.name = firstname WHERE state = 'WA' OR id < 7"),
        query(
            "SELECT opensearch-sql_test_index_order_0.id, b.firstname ",
            "FROM opensearch-sql_test_index_order  opensearch-sql_test_index_order_0 ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON opensearch-sql_test_index_order_0.name = b.firstname ",
            "WHERE b.state = 'WA' OR opensearch-sql_test_index_order_0.id < 7"));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void actualTableNameAsAliasOnColumnFieldsTwo() throws IOException {
    sameExplain(
        query(
            "SELECT opensearch-sql_test_index_order.id, opensearch-sql_test_index_bank.firstname ",
            "FROM opensearch-sql_test_index_order ",
            "INNER JOIN opensearch-sql_test_index_bank ",
            "ON opensearch-sql_test_index_order.name = firstname ",
            "WHERE opensearch-sql_test_index_bank.state = 'WA' OR id < 7"),
        query(
            "SELECT opensearch-sql_test_index_order_0.id,"
                + " opensearch-sql_test_index_bank_1.firstname ",
            "FROM opensearch-sql_test_index_order  opensearch-sql_test_index_order_0 ",
            "INNER JOIN opensearch-sql_test_index_bank opensearch-sql_test_index_bank_1",
            "ON opensearch-sql_test_index_order_0.name = opensearch-sql_test_index_bank_1.firstname"
                + " ",
            "WHERE opensearch-sql_test_index_bank_1.state = 'WA' OR"
                + " opensearch-sql_test_index_order_0.id < 7"));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void columnsWithTableAliasNotAffected() throws IOException {
    sameExplain(
        query(
            "SELECT a.id, firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON name = b.firstname WHERE state = 'WA' OR a.id < 7"),
        query(
            "SELECT a.id, b.firstname ",
            "FROM opensearch-sql_test_index_order a ",
            "INNER JOIN opensearch-sql_test_index_bank b ",
            "ON a.name = b.firstname ",
            "WHERE b.state = 'WA' OR a.id < 7"));
  }

  @Test
  @RequiresCapability(QUERY_ERROR_MESSAGE)
  public void commonColumnWithoutTableAliasDifferentTables() throws IOException {
    exception.expect(ResponseException.class);
    exception.expectMessage("Field name [firstname] is ambiguous");
    String explain =
        explainQuery(
            query(
                "SELECT firstname, lastname ",
                "FROM opensearch-sql_test_index_bank ",
                "LEFT JOIN opensearch-sql_test_index_bank_two ",
                "ON firstname = lastname WHERE state = 'VA' "));
  }

  @Test
  @RequiresCapability(QUERY_ERROR_MESSAGE)
  public void sameTablesNoAliasAndNoAliasOnColumns() throws IOException {
    exception.expect(ResponseException.class);
    exception.expectMessage("Not unique table/alias: [opensearch-sql_test_index_bank]");
    String explain =
        explainQuery(
            query(
                "SELECT firstname, lastname ",
                "FROM opensearch-sql_test_index_bank ",
                "LEFT JOIN opensearch-sql_test_index_bank ",
                "ON firstname = lastname WHERE state = 'VA' "));
  }

  @Test
  @RequiresCapability(QUERY_ERROR_MESSAGE)
  public void sameTablesNoAliasWithTableNameAsAliasOnColumns() throws IOException {
    exception.expect(ResponseException.class);
    exception.expectMessage("Not unique table/alias: [opensearch-sql_test_index_bank]");
    String explain =
        explainQuery(
            query(
                "SELECT opensearch-sql_test_index_bank.firstname",
                "FROM opensearch-sql_test_index_bank ",
                "JOIN opensearch-sql_test_index_bank ",
                "ON opensearch-sql_test_index_bank.firstname ="
                    + " opensearch-sql_test_index_bank.lastname"));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void sameTablesWithExplicitAliasOnFirst() throws IOException {
    sameExplain(
        query(
            "SELECT opensearch-sql_test_index_bank.firstname, a.lastname ",
            "FROM opensearch-sql_test_index_bank a",
            "JOIN opensearch-sql_test_index_bank ",
            "ON opensearch-sql_test_index_bank.firstname = a.lastname "),
        query(
            "SELECT opensearch-sql_test_index_bank_0.firstname, a.lastname ",
            "FROM opensearch-sql_test_index_bank a",
            "JOIN  opensearch-sql_test_index_bank opensearch-sql_test_index_bank_0",
            "ON opensearch-sql_test_index_bank_0.firstname = a.lastname "));
  }

  @Test
  @RequiresCapability(EXPLAIN_FORMAT)
  public void sameTablesWithExplicitAliasOnSecond() throws IOException {
    sameExplain(
        query(
            "SELECT opensearch-sql_test_index_bank.firstname, a.lastname ",
            "FROM opensearch-sql_test_index_bank ",
            "JOIN opensearch-sql_test_index_bank a",
            "ON opensearch-sql_test_index_bank.firstname = a.lastname "),
        query(
            "SELECT opensearch-sql_test_index_bank_0.firstname, a.lastname ",
            "FROM opensearch-sql_test_index_bank opensearch-sql_test_index_bank_0",
            "JOIN  opensearch-sql_test_index_bank a",
            "ON opensearch-sql_test_index_bank_0.firstname = a.lastname "));
  }

  private void sameExplain(String actualQuery, String expectedQuery) throws IOException {
    assertThat(explainQuery(actualQuery), equalTo(explainQuery(expectedQuery)));
  }

  private String query(String... statements) {
    return String.join(" ", statements);
  }
}
