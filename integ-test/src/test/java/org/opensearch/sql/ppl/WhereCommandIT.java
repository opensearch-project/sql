/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.hamcrest.MatcherAssert;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class WhereCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.GAME_OF_THRONES);
  }

  @Test
  public void testWhereWithLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where firstname='Amber' | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithMultiLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where firstname='Amber' lastname='Duke' age=32 "
                    + "| fields firstname, lastname, age",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber", "Duke", 32));
  }

  @Test
  public void testMultipleWhereCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where firstname='Amber' "
                    + "| fields lastname, age"
                    + "| where lastname='Duke' "
                    + "| fields age "
                    + "| where age=32 "
                    + "| fields age",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testWhereEquivalentSortCommand() throws IOException {
    assertEquals(
        executeQueryToString(
            String.format("source=%s | where firstname='Amber'", TEST_INDEX_ACCOUNT)),
        executeQueryToString(String.format("source=%s firstname='Amber'", TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testLikeFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where like(firstname, 'Ambe_') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testIsNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where isnull(age) | fields firstname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(result, rows("Virginia"));
  }

  @Test
  public void testIsNotNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(age) and like(firstname, 'Ambe_') | fields firstname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(result, rows("Amber JOHnny"));
  }

  @Test
  public void testWhereWithMetadataFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | where _id='1' | fields firstname", TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithIn() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where firstname in ('Amber') | fields firstname", TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));

    result =
        executeQuery(
            String.format(
                "source=%s | where firstname in ('Amber', 'Dale') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"), rows("Dale"));

    result =
        executeQuery(
            String.format(
                "source=%s | where balance in (4180, 5686.0) | fields balance",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows(4180), rows(5686));
  }

  @Test
  public void testWhereWithNotIn() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where account_number < 4 | where firstname not in ('Amber', 'Levine')"
                    + " | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Roberta"), rows("Bradshaw"));

    result =
        executeQuery(
            String.format(
                "source=%s | where account_number < 4 | where not firstname in ('Amber', 'Levine')"
                    + " | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Roberta"), rows("Bradshaw"));

    result =
        executeQuery(
            String.format(
                "source=%s | where not firstname not in ('Amber', 'Dale') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"), rows("Dale"));
  }

  @Test
  public void testInWithIncompatibleType() {
    Exception e =
        assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | where balance in (4180, 5686, '6077') | fields firstname",
                      TEST_INDEX_ACCOUNT));
            });
    MatcherAssert.assertThat(e.getMessage(), containsString(getIncompatibleTypeErrMsg()));
  }

  protected String getIncompatibleTypeErrMsg() {
    return "function expected"
               + " {[BYTE,BYTE],[SHORT,SHORT],[INTEGER,INTEGER],[LONG,LONG],[FLOAT,FLOAT],[DOUBLE,DOUBLE],[STRING,STRING],[BOOLEAN,BOOLEAN],[DATE,DATE],[TIME,TIME],[TIMESTAMP,TIMESTAMP],[INTERVAL,INTERVAL],[IP,IP],[STRUCT,STRUCT],[ARRAY,ARRAY]},"
               + " but got [LONG,STRING]";
  }
}
