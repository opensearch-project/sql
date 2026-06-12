/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assume.assumeFalse;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLBasicIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // The parquet/composite store on the analytics-engine route is append-only: re-PUTting the same
    // _id adds a row instead of replacing it. init() runs as @Before (before every test), so
    // re-seeding these raw-document indices each time would inflate their row counts. Seed once,
    // mirroring loadIndex's isIndexExist guard; v2 behavior is unchanged (same end state).
    if (!isIndexExist(client(), "test")) {
      Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
      request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
      client().performRequest(request1);
      Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
      request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
      client().performRequest(request2);
    }
    // PUT index test1
    if (!isIndexExist(client(), "test1")) {
      Request request3 = new Request("PUT", "/test1/_doc/1?refresh=true");
      request3.setJsonEntity("{\"name\": \"HELLO\", \"alias\": \"Hello\"}");
      client().performRequest(request3);
    }

    loadIndex(Index.BANK);
    loadIndex(Index.DATA_TYPE_ALIAS);
    loadIndex(Index.MERGE_TEST_1);
    loadIndex(Index.MERGE_TEST_2);
  }

  @Test
  public void testInvalidTable() {
    if (isAnalyticsParquetIndicesEnabled()) {
      // The analytics-engine route resolves tables through Calcite's catalog, which raises a
      // CalciteException ("Table 'unknown' not found", surfaced as HTTP 400) rather than the v2
      // path's IllegalStateException ("no such index [unknown]").
      Throwable e =
          assertThrowsWithReplace(ResponseException.class, () -> executeQuery("source=unknown"));
      verifyErrorMessageContains(e, "Table 'unknown' not found");
      return;
    }
    Throwable e =
        assertThrowsWithReplace(IllegalStateException.class, () -> executeQuery("source=unknown"));
    verifyErrorMessageContains(e, "no such index [unknown]");
  }

  @Test
  public void testSourceQuery() throws IOException {
    // Pin the projection with an explicit `| fields`. The analytics-engine route returns SELECT-*
    // columns in parquet storage order (alphabetical), not the v2 path's mapping order, so an
    // unpinned `source=test` yields [age, name]. Pinning makes column order deterministic across
    // both engines without changing which rows are returned.
    JSONObject actual = executeQuery("source=test | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testMultipleSourceQuery_SameTable() throws IOException {
    // Pin column order — see testSourceQuery (analytics route returns storage/alphabetical order).
    JSONObject actual = executeQuery("source=test, test | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testMultipleSourceQuery_DifferentTables() throws IOException {
    // Pin column order — see testSourceQuery (analytics route returns storage/alphabetical order).
    JSONObject actual = executeQuery("source=test, test1 | fields name, alias, age");
    verifySchema(
        actual, schema("name", "string"), schema("age", "bigint"), schema("alias", "string"));
    verifyDataRows(
        actual, rows("hello", null, 20), rows("world", null, 30), rows("HELLO", "Hello", null));
  }

  @Test
  public void testIndexPatterns() throws IOException {
    // Pin column order — see testSourceQuery (analytics route returns storage/alphabetical order).
    JSONObject actual = executeQuery("source=test* | fields name, alias, age");
    verifySchema(
        actual, schema("name", "string"), schema("age", "bigint"), schema("alias", "string"));
    verifyDataRows(
        actual, rows("hello", null, 20), rows("world", null, 30), rows("HELLO", "Hello", null));
  }

  @Test
  public void testSourceFieldQuery() throws IOException {
    JSONObject actual = executeQuery("source=test | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("hello"), rows("world"));
  }

  @Test
  public void testFieldsShouldBeCaseSensitive() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalStateException.class, () -> executeQuery("source=test | fields NAME"));
    verifyErrorMessageContains(e, "Field [NAME] not found.");
  }

  @Test
  public void testFilterQuery1() throws IOException {
    JSONObject actual = executeQuery("source=test | where age = 30 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("world", 30));
  }

  @Test
  public void testFilterQuery2() throws IOException {
    JSONObject actual = executeQuery("source=test | where age = 20 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20));
  }

  @Test
  public void testFilterQuery3() throws IOException {
    JSONObject actual =
        executeQuery("source=test | where age > 10 AND age < 100 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testFilterQuery4() throws IOException {
    JSONObject actual = executeQuery("source=test | where age = 20.0 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20));
  }

  @Test
  public void testRegexpFilter() throws IOException {
    JSONObject actual = executeQuery("source=test | where name REGEXP 'he.*' | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20));
  }

  @Test
  public void testFilterOnTextField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where gender = 'F' | fields firstname, lastname", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(
        actual, rows("Nanette", "Bates"), rows("Virginia", "Ayala"), rows("Dillard", "Mcpherson"));
  }

  @Test
  public void testFilterOnTextFieldWithKeywordSubField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where state = 'VA' | fields firstname, lastname", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(actual, rows("Nanette", "Bates"));
  }

  @Test
  public void testFilterQueryWithOr() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                    + " fields firstname, lastname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(actual, rows("Amber JOHnny", "Duke Willmington"), rows("Elinor", "Ratliff"));
  }

  @Test
  public void testFilterQueryWithOr2() throws IOException {
    assumeFalse(
        "The implicit search-filter syntax (source=idx (cond)) lowers to a Lucene query_string,"
            + " which the DataFusion backend doesn't support; it matches no rows on the"
            + " analytics-engine route. The explicit `| where` form (testFilterQueryWithOr) works"
            + " there.",
        isAnalyticsParquetIndicesEnabled());
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                    + " fields firstname, lastname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(actual, rows("Amber JOHnny", "Duke Willmington"), rows("Elinor", "Ratliff"));
  }

  @Test
  public void testQueryMinusFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                // Trailing `| fields` pins the post-exclusion column order — the analytics-engine
                // route returns the surviving columns in storage (alphabetical) order, not the v2
                // path's mapping order. The `fields -` exclusion is still the clause under test.
                "source=%s | fields - firstname, lastname, birthdate"
                    + " | fields account_number, address, gender, city, balance, employer, state,"
                    + " age, email, male",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("address", "string"),
        schema("gender", "string"),
        schema("city", "string"),
        schema("balance", "bigint"),
        schema("employer", "string"),
        schema("state", "string"),
        schema("age", "int"),
        schema("email", "string"),
        schema("male", "boolean"));
    verifyDataRows(
        actual,
        rows(
            1,
            "880 Holmes Lane",
            "M",
            "Brogan",
            39225,
            "Pyrami",
            "IL",
            32,
            "amberduke@pyrami.com",
            true),
        rows(
            6,
            "671 Bristol Street",
            "M",
            "Dante",
            5686,
            "Netagy",
            "TN",
            36,
            "hattiebond@netagy.com",
            true),
        rows(
            13,
            "789 Madison Street",
            "F",
            "Nogal",
            32838,
            "Quility",
            "VA",
            28,
            "nanettebates@quility.com",
            false),
        rows(
            18,
            "467 Hutchinson Court",
            "M",
            "Orick",
            4180,
            "Boink",
            "MD",
            33,
            "daleadams@boink.com",
            true),
        rows(
            20,
            "282 Kings Place",
            "M",
            "Ribera",
            16418,
            "Scentric",
            "WA",
            36,
            "elinorratliff@scentric.com",
            true),
        rows(
            25,
            "171 Putnam Avenue",
            "F",
            "Nicholson",
            40540,
            "Filodyne",
            "PA",
            39,
            "virginiaayala@filodyne.com",
            false),
        rows(
            32,
            "702 Quentin Street",
            "F",
            "Veguita",
            48086,
            "Quailcom",
            "IN",
            34,
            "dillardmcpherson@quailcom.com",
            false));
  }

  @Test
  public void testQueryMinusFieldsWithFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                // Trailing `| fields` pins the post-exclusion column order — see
                // testQueryMinusFields.
                "source=%s | where (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                    + " fields - firstname, lastname"
                    + " | fields account_number, address, birthdate, gender, city, balance,"
                    + " employer, state, age, email, male",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("address", "string"),
        schema("birthdate", "timestamp"),
        schema("gender", "string"),
        schema("city", "string"),
        schema("balance", "bigint"),
        schema("employer", "string"),
        schema("state", "string"),
        schema("age", "int"),
        schema("email", "string"),
        schema("male", "boolean"));
    verifyDataRows(
        actual,
        rows(
            1,
            "880 Holmes Lane",
            "2017-10-23 00:00:00",
            "M",
            "Brogan",
            39225,
            "Pyrami",
            "IL",
            32,
            "amberduke@pyrami.com",
            true),
        rows(
            20,
            "282 Kings Place",
            "2018-06-27 00:00:00",
            "M",
            "Ribera",
            16418,
            "Scentric",
            "WA",
            36,
            "elinorratliff@scentric.com",
            true));
  }

  @Test
  public void testFieldsPlusThenMinus() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields + firstname, lastname, account_number | fields - firstname,"
                    + " lastname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("account_number", "bigint"));
    verifyDataRows(actual, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testMultipleTables_SameTable() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s, %s | stats count() as c", TEST_INDEX_BANK, TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testMultipleTablesAndFilters_SameTable() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s, %s gender = 'F' | stats count() as c",
                TEST_INDEX_BANK, TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(3));
  }

  @Test
  public void testMultipleTables_DifferentTables() throws IOException {
    assumeFalse(
        "Multi-index source with a numeric field of conflicting widths (bank.age=integer,"
            + " test.age=long) is rejected by the analytics-engine schema merge (no integer->long"
            + " widening) instead of being coerced like the v2/Calcite path.",
        isAnalyticsParquetIndicesEnabled());
    JSONObject actual =
        executeQuery(String.format("source=%s, test | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(9));
  }

  @Test
  public void testMultipleTables_WithIndexPattern() throws IOException {
    assumeFalse(
        "Multi-index source with conflicting numeric widths (bank.age=integer, test.age=long) is"
            + " rejected by the analytics-engine schema merge (no integer->long widening).",
        isAnalyticsParquetIndicesEnabled());
    JSONObject actual =
        executeQuery(String.format("source=%s, test* | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(10));
  }

  @Test
  public void testMultipleTablesAndFilters_WithIndexPattern() throws IOException {
    assumeFalse(
        "Multi-index source with conflicting numeric widths (bank.age=integer, test.age=long) is"
            + " rejected by the analytics-engine schema merge (no integer->long widening).",
        isAnalyticsParquetIndicesEnabled());
    JSONObject actual =
        executeQuery(
            String.format("source=%s, test* gender = 'F' | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(3));
  }

  @Test
  public void testSelectDateTypeField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | fields birthdate", TEST_INDEX_BANK));
    verifySchema(actual, schema("birthdate", "timestamp"));
    verifyDataRows(
        actual,
        rows("2017-10-23 00:00:00"),
        rows("2017-11-20 00:00:00"),
        rows("2018-06-23 00:00:00"),
        rows("2018-11-13 23:33:20"),
        rows("2018-06-27 00:00:00"),
        rows("2018-08-19 00:00:00"),
        rows("2018-08-11 00:00:00"));
  }

  @Test
  public void testAllFieldsInTable() throws IOException {
    Request request = new Request("PUT", "/a/_doc/1?refresh=true");
    request.setJsonEntity("{\"name\": \"hello\"}");
    client().performRequest(request);

    JSONObject actual = executeQuery("source=a | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("hello"));
  }

  @Test
  public void testBetween() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 35 and 38 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithExpression() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 36 - 1 and 37 + 1 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithDifferentTypes() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 35.5 and 38.5 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithDifferentTypes2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 35 and 38.5 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithIncompatibleTypes() {
    Throwable e =
        assertThrowsWithReplace(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | where age between '35' and 38.5 | fields firstname, age",
                        TEST_INDEX_BANK)));
    verifyErrorMessageContains(e, "BETWEEN expression types are incompatible");
  }

  @Test
  public void testNotBetween() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age not between 30 and 39 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Nanette", 28));
  }

  @Test
  public void testNotBetween2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where not age between 30 and 39 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Nanette", 28));
  }

  @Test
  public void testNotBetween3() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where not age not between 35 and 38 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testDateBetween() throws IOException {
    // birthdate is a TIMESTAMP-typed field; the bounds are DATE literals. BETWEEN must coerce the
    // mixed temporal operands rather than reject them with "expression types are incompatible".
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where birthdate between date('2018-06-01') and date('2018-06-30') |"
                    + " fields firstname, birthdate",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("birthdate", "timestamp"));
    verifyDataRows(
        actual, rows("Nanette", "2018-06-23 00:00:00"), rows("Elinor", "2018-06-27 00:00:00"));
  }

  /**
   * A timestamp range comparison AND'd with an {@code IN} clause must push down and return rows.
   */
  @Test
  public void testTimestampRangeWithInClausePushDown() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where birthdate > timestamp('2018-06-01 00:00:00') | where state in"
                    + " ('IL', 'TN', 'WA') | fields firstname, state, birthdate",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("state", "string"),
        schema("birthdate", "timestamp"));
    verifyDataRows(actual, rows("Elinor", "WA", "2018-06-27 00:00:00"));
  }

  @Test
  public void testDateIn() throws IOException {
    // birthdate is a TIMESTAMP-typed field; the IN values are DATE literals. visitIn must compare
    // each value in the field's temporal domain (via PPL `=`) rather than letting leastRestrictive
    // collapse the common type to VARCHAR and string-compare mismatched renderings — which silently
    // matched nothing without pushdown. See visitIn in CalciteRexNodeVisitor.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where birthdate in (DATE '2018-06-23', DATE '2018-06-27') |"
                    + " fields firstname, birthdate",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("birthdate", "timestamp"));
    verifyDataRows(
        actual, rows("Nanette", "2018-06-23 00:00:00"), rows("Elinor", "2018-06-27 00:00:00"));
  }

  @Test
  public void testDateNotIn() throws IOException {
    // Complement of testDateIn: NOT IN over a temporal field. Exercises the complemented-points
    // pushdown branch, which must also format timestamp values rather than emit a flat terms query.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where birthdate not in (DATE '2018-06-23', DATE '2018-06-27') |"
                    + " fields firstname | sort firstname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"));
    verifyDataRows(
        actual,
        rows("Amber JOHnny"),
        rows("Dale"),
        rows("Dillard"),
        rows("Hattie"),
        rows("Virginia"));
  }

  @Test
  public void testIn() throws IOException {
    // Non-temporal IN keeps the leastRestrictive + makeIn path; guards against the temporal-field
    // special-case in visitIn regressing membership tests over ordinary columns.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age in (32, 36) | fields firstname, age", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Amber JOHnny", 32), rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testXor() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where firstname='Hattie' xor age=36 | fields firstname, age",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Elinor", 36));
  }

  @Test
  public void testKeepThrowCalciteException() throws IOException {
    Class<? extends Exception> expectedException =
        isStandaloneTest() ? IllegalArgumentException.class : ResponseException.class;
    withFallbackEnabled(
        () -> {
          Throwable e =
              assertThrowsWithReplace(
                  IllegalArgumentException.class,
                  () ->
                      executeQuery(
                          String.format("source=%s | fields firstname1, age", TEST_INDEX_BANK)));
          verifyErrorMessageContains(e, "Field [firstname1] not found.");
        },
        "");
  }

  @Test
  public void testAliasDataType() throws IOException {
    assumeFalse(
        "alias-typed fields are stripped from test datasets on the analytics-engine route (the"
            + " parquet/composite store can't hold them), so alias_col doesn't exist there.",
        isAnalyticsParquetIndicesEnabled());
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where alias_col > 1 | fields original_col, alias_col ",
                TEST_INDEX_ALIAS));
    verifySchema(result, schema("original_col", "int"), schema("alias_col", "int"));
    verifyDataRows(result, rows(2, 2), rows(3, 3));
  }

  @Test
  public void testMetaFieldAlias() throws IOException {
    Throwable e =
        assertThrowsWithReplace(
            Exception.class,
            () ->
                executeQuery(
                    String.format("source=%s | stats count() as _score", TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e, "Cannot use metadata field [_score] as the alias.");
  }

  @Test
  public void testFieldsMergedObject() throws IOException {
    assumeFalse(
        "This projects machine_array.* (a nested field, stripped from datasets on the"
            + " analytics-engine route) and relies on cross-index object-field merge over the"
            + " merge_test* wildcard, which the analytics-engine route doesn't resolve.",
        isAnalyticsParquetIndicesEnabled());
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields machine.os1,  machine.os2, machine_array.os1, "
                    + " machine_array.os2, machine_deep.attr1, machine_deep.attr2,"
                    + " machine_deep.layer.os1, machine_deep.layer.os2",
                TEST_INDEX_MERGE_TEST_WILDCARD));
    verifySchema(
        result,
        schema("machine.os1", "string"),
        schema("machine.os2", "string"),
        schema("machine_array.os1", "string"),
        schema("machine_array.os2", "string"),
        schema("machine_deep.attr1", "bigint"),
        schema("machine_deep.attr2", "bigint"),
        schema("machine_deep.layer.os1", "string"),
        schema("machine_deep.layer.os2", "string"));
    verifyDataRows(
        result,
        rows("linux", null, "linux", null, 1, null, "os1", null),
        rows(null, "linux", null, "linux", null, 2, null, "os2"));
  }

  public void testNumericLiteral() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test | eval decimalLiteral = 0.06 - 0.01, doubleLiteral = 0.06d - 0.01d,"
                + " floatLiteral = 0.06f - 0.01f"
                // Pin column order — see testSourceQuery (analytics route reorders columns).
                + " | fields name, age, decimalLiteral, doubleLiteral, floatLiteral");
    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "bigint"),
        schema("decimalLiteral", "double"),
        schema("doubleLiteral", "double"),
        schema("floatLiteral", "float"));
    verifyDataRows(
        result,
        rows("hello", 20, 0.05, 0.049999999999999996, 0.049999999999999996),
        rows("world", 30, 0.05, 0.049999999999999996, 0.049999999999999996));
  }

  @Test
  public void testDecimalLiteral() throws IOException {
    assumeFalse(
        "Non-suffixed decimal literals use DECIMAL arithmetic on the v2/Calcite path but DOUBLE on"
            + " the DataFusion backend (e.g. 0.1 / 0.3 * 0.3 = 0.1 vs 0.0999...), so these"
            + " precision-sensitive expectations diverge on the analytics-engine route.",
        isAnalyticsParquetIndicesEnabled());
    JSONObject result =
        executeQuery(
            "source=test | eval r1 = 22 / 7.0, r2 = 22 / 7.0d, r3 = 22.0 / 7, r4 = 22.0d / 7,"
                + " r5 = 0.1 * 0.2, r6 = 0.1d * 0.2d, r7 = 0.1 + 0.2, r8 = 0.1d + 0.2d,"
                + " r9 = 0.06 - 0.01, r10 = 0.06d - 0.01d, r11 = 0.1 / 0.3 * 0.3,"
                + " r12 = 0.1d / 0.3d * 0.3d, r13 = pow(sqrt(2.0), 2), r14 = pow(sqrt(2.0d), 2),"
                + " r15 = 7.0 / 0, r16 = 7 / 0.0");
    verifyDataRows(
        result,
        rows(
            "hello",
            20,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            0.02,
            0.020000000000000004,
            0.3,
            0.30000000000000004,
            0.05,
            0.049999999999999996,
            0.1,
            0.1,
            2.0000000000000004,
            2.0000000000000004,
            null,
            null),
        rows(
            "world",
            30,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            0.02,
            0.020000000000000004,
            0.3,
            0.30000000000000004,
            0.05,
            0.049999999999999996,
            0.1,
            0.1,
            2.0000000000000004,
            2.0000000000000004,
            null,
            null));
  }
}
