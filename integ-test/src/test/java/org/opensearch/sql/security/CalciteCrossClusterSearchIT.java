/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONObject;
import org.junit.Test;

/** Cross Cluster Search tests with Calcite enabled for enhanced fields features. */
public class CalciteCrossClusterSearchIT extends CrossClusterTestBase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK, remoteClient());
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.ACCOUNT, remoteClient());
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.TIME_TEST_DATA, remoteClient());
    loadIndex(Index.MVEXPAND_EDGE_CASES);
    loadIndex(Index.MVEXPAND_EDGE_CASES, remoteClient());
    enableCalcite();
  }

  @Test
  public void testCrossClusterFieldsSpaceDelimited() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s | fields dog_name age", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("age"));
    verifySchema(result, schema("dog_name", "string"), schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterFieldsWildcardPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields dog*", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"));
    verifySchema(result, schema("dog_name", "string"));
  }

  @Test
  public void testCrossClusterFieldsWildcardSuffix() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields *Name", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("holdersName"));
    verifySchema(result, schema("holdersName", "string"));
  }

  @Test
  public void testCrossClusterFieldsMixedDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | fields dog_name, age holdersName", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("age"), columnName("holdersName"));
    verifySchema(
        result,
        schema("dog_name", "string"),
        schema("age", "bigint"),
        schema("holdersName", "string"));
  }

  @Test
  public void testCrossClusterTableCommand() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | table dog_name age", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("age"));
    verifySchema(result, schema("dog_name", "string"), schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterFieldsAllWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields *", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
    verifySchema(
        result,
        schema("dog_name", "string"),
        schema("holdersName", "string"),
        schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterFieldsExclusion() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields - age", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"));
    verifySchema(result, schema("dog_name", "string"), schema("holdersName", "string"));
  }

  @Test
  public void testCrossClusterTableWildcardPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | table first*", TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("firstname"));
    verifySchema(result, schema("firstname", "string"));
  }

  @Test
  public void testCrossClusterFieldsAndTableEquivalence() throws IOException {
    JSONObject fieldsResult =
        executeQuery(
            String.format("search source=%s | fields dog_name age", TEST_INDEX_DOG_REMOTE));
    JSONObject tableResult =
        executeQuery(String.format("search source=%s | table dog_name age", TEST_INDEX_DOG_REMOTE));

    verifyColumn(fieldsResult, columnName("dog_name"), columnName("age"));
    verifyColumn(tableResult, columnName("dog_name"), columnName("age"));
    verifySchema(fieldsResult, schema("dog_name", "string"), schema("age", "bigint"));
    verifySchema(tableResult, schema("dog_name", "string"), schema("age", "bigint"));
  }

  @Test
  public void testDefaultBinCrossCluster() throws IOException {
    // Default bin without any parameters
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451, "20.0-30.0"), rows(504L, "30.0-40.0"), rows(45L, "40.0-50.0"));
  }

  @Test
  public void testSpanBinCrossCluster() throws IOException {
    // Span-based binning
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testCountBinCrossCluster() throws IOException {
    // Count-based binning (bins parameter)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testMinSpanBinCrossCluster() throws IOException {
    // MinSpan-based binning
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 start=0 end=100 | stats count() by age | sort age |"
                    + " head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testRangeBinCrossCluster() throws IOException {
    // Range-based binning (start/end only)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=100 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testTimeBinCrossCluster() throws IOException {
    // Time-based binning with span
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin @timestamp span=1h | fields `@timestamp`, value | sort"
                    + " `@timestamp` | head 3",
                TEST_INDEX_TIME_DATA_REMOTE));
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));

    // With 1-hour spans
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00", 8945),
        rows("2025-07-28 01:00:00", 7623),
        rows("2025-07-28 02:00:00", 9187));
  }

  @Test
  public void testCrossClusterRegexBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | regex firstname='.*att.*' | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterRegexWithNegation() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | regex firstname!='.*att.*' | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result,
        rows("Virginia"),
        rows("Elinor"),
        rows("Dillard"),
        rows("Dale"),
        rows("Amber JOHnny"),
        rows("Nanette"));
  }

  @Test
  public void testCrossClusterRenameWildcardPattern() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s | rename *ame as *AME", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_nAME"), columnName("holdersNAME"), columnName("age"));
    verifySchema(
        result,
        schema("dog_nAME", "string"),
        schema("holdersNAME", "string"),
        schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterRenameFullWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | rename * as old_*", TEST_INDEX_DOG_REMOTE));
    verifyColumn(
        result,
        columnName("old_dog_name"),
        columnName("old_holdersName"),
        columnName("old_age"),
        columnName("old__id"),
        columnName("old__index"),
        columnName("old__score"),
        columnName("old__maxscore"),
        columnName("old__sort"),
        columnName("old__routing"));
    verifySchema(
        result,
        schema("old_dog_name", "string"),
        schema("old_holdersName", "string"),
        schema("old_age", "bigint"),
        schema("old__id", "string"),
        schema("old__index", "string"),
        schema("old__score", "float"),
        schema("old__maxscore", "float"),
        schema("old__sort", "bigint"),
        schema("old__routing", "string"));
  }

  @Test
  public void testCrossClusterRexBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=firstname \\\"(?<initial>^[A-Z])\\\" | fields"
                    + " firstname, initial | head 3",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Amber JOHnny", "A"), rows("Hattie", "H"), rows("Nanette", "N"));
  }

  @Test
  public void testCrossClusterRexMultipleGroups() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=lastname \\\"(?<first>[A-Z])(?<rest>[a-z]+)\\\" |"
                    + " fields lastname, first, rest | head 2",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Duke Willmington", "D", "uke"), rows("Bond", "B", "ond"));
  }

  @Test
  public void testCrossClusterRexSedMode() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=firstname mode=sed \\\"s/^[A-Z]/X/\\\" | fields"
                    + " firstname | head 3",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Xmber JOHnny"), rows("Xattie"), rows("Xanette"));
  }

  @Test
  public void testCrossClusterRexWithMaxMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=firstname \\\"(?<letter>[A-Z])\\\" max_match=2 |"
                    + " fields firstname, letter | head 2",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result, rows("Amber JOHnny", new String[] {"A", "J"}), rows("Hattie", new String[] {"H"}));
  }

  @Test
  public void testCrossClusterRexWithOffsetField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=lastname \\\"(?<vowel>[aeiou])\\\" offset_field=pos |"
                    + " fields lastname, vowel, pos | head 2",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result, rows("Duke Willmington", "u", "vowel=1-1"), rows("Bond", "o", "vowel=1-1"));
  }

  @Test
  public void testCrossClusterAddTotals() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s| sort 1 age | fields firstname, age | addtotals age",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Nanette", 28, 28));
  }

  /** CrossClusterSearchIT Test for addcoltotals. */
  @Test
  public void testCrossClusterAddColTotals() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where  firstname='Hattie' or firstname ='Nanette'|fields"
                    + " firstname,age,balance | addcoltotals age balance",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result, rows("Hattie", 36, 5686), rows("Nanette", 28, 32838), rows(null, 64, 38524));
  }

  @Test
  public void testCrossClusterTranspose() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where  firstname='Hattie' or firstname ='Nanette' or"
                    + " firstname='Dale'|sort firstname desc |fields firstname,age,balance |"
                    + " transpose 3 column_name='column_names'",
                TEST_INDEX_BANK_REMOTE));

    verifyDataRows(
        result,
        rows("firstname", "Nanette", "Hattie", "Dale"),
        rows("balance", "32838", "5686", "4180"),
        rows("age", "28", "36", "33"));
  }

  @Test
  public void testCrossClusterAppend() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | stats count() as cnt by gender | append [ search source=%s |"
                    + " stats count() as cnt ]",
                TEST_INDEX_BANK_REMOTE, TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows(3, "F"), rows(4, "M"), rows(7, null));
  }

  /** CrossClusterSearchIT Test for mvcombine. */
  @Test
  public void testCrossClusterMvcombine() throws IOException {

    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where firstname='Hattie' or firstname='Nanette' "
                    + "| fields firstname, age | mvcombine age",
                TEST_INDEX_BANK_REMOTE));

    verifyColumn(result, columnName("firstname"), columnName("age"));

    verifyDataRows(
        result,
        rows("Hattie", new org.json.JSONArray().put(36)),
        rows("Nanette", new org.json.JSONArray().put(28)));
  }

  /** CrossClusterSearchIT Test for fieldformat. */
  @Test
  public void testCrossClusterFieldFormat() throws IOException {
    // Test fieldformat command with tostring
    JSONObject result =
        executeQuery(
            StringEscapeUtils.escapeJson(
                String.format(
                    "search source=%s | where  firstname='Hattie' or firstname ='Nanette'|fields"
                        + " firstname,age,balance | fieldformat formatted_balance ="
                        + " \"$\".tostring(balance,\"commas\")",
                    TEST_INDEX_BANK_REMOTE)));
    verifyDataRows(
        result, rows("Hattie", 36, 5686, "$5,686"), rows("Nanette", 28, 32838, "$32,838"));
  }

  /** CrossClusterSearchIT Test for nomv. */
  @Test
  public void testCrossClusterNoMv() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where firstname='Hattie' "
                    + "| eval names = array(firstname, lastname) | nomv names "
                    + "| fields firstname, names",
                TEST_INDEX_BANK_REMOTE));

    verifyColumn(result, columnName("firstname"), columnName("names"));
    verifySchema(result, schema("firstname", "string"), schema("names", "string"));

    verifyDataRows(result, rows("Hattie", "Hattie\nBond"));
  }

  @Test
  public void testCrossClusterConvert() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) | fields balance",
                TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("balance"));
    verifySchema(result, schema("balance", "double"));
  }

  @Test
  public void testCrossClusterConvertWithAlias() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) AS balance_num | fields balance_num",
                TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("balance_num"));
    verifySchema(result, schema("balance_num", "double"));
  public void testCrossClusterMvExpandBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | mvexpand skills | where username='happy' | fields username,"
                    + " skills.name | sort skills.name",
                TEST_INDEX_MVEXPAND_REMOTE));
    verifySchema(result, schema("username", "string"), schema("skills.name", "string"));
    verifyDataRows(result, rows("happy", "java"), rows("happy", "python"), rows("happy", "sql"));
  }

  @Test
  public void testCrossClusterMvExpandWithLimit() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | mvexpand skills limit=2 | where username='limituser' | fields"
                    + " username, skills.name | sort skills.name",
                TEST_INDEX_MVEXPAND_REMOTE));
    verifySchema(result, schema("username", "string"), schema("skills.name", "string"));
    verifyDataRows(result, rows("limituser", "a"), rows("limituser", "b"));
  }
}
