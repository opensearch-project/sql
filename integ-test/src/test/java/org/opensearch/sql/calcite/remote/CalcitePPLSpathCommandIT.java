/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLSpathCommandIT extends CalcitePPLSpathTestBase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    putJsonItem(1, "simple", sj("{'a': 1, 'b': 2, 'c': 3}"));
    putJsonItem(2, "simple", sj("{'a': 1, 'b': 2, 'c': 3}"));
    putJsonItem(3, "nested", sj("{'nested': {'d': [1, 2, 3], 'e': 'str'}}"));
    putJsonItem(4, "overwrap", sj("{'a.b': 1, 'a': {'b': 2, 'c': 3}}"));
    putJsonItem(
        5, "types", sj("{'string': 'STRING', 'boolean': true, 'number': 10.1, 'null': null}"));
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData output=result path=a"
                + " | fields result | head 2");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("1"));
  }

  private static final String EXPECTED_SPATH_WILDCARD_ERROR =
      "Spath command cannot be used with partial wildcard such as `prefix*`.";

  @Test
  public void testSpathWithWildcard() throws IOException {
    verifyExplainException(
        "source=test_json | spath input=userData | fields a, b*", EXPECTED_SPATH_WILDCARD_ERROR);
  }

  @Test
  public void testSpathWithoutFields() throws IOException {
    JSONObject result =
        executeQuery("source=test_json | where category='simple' | spath input=userData | head 1");
    verifySchema(
        result,
        schema("a", "string"),
        schema("b", "string"),
        schema("c", "string"),
        schema("category", "string"),
        schema("userData", "string"));
    verifyDataRows(result, rows("1", "2", "3", "simple", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testSpathWithOnlyWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | fields * | head"
                + " 1");
    verifySchema(
        result,
        schema("a", "string"),
        schema("b", "string"),
        schema("c", "string"),
        schema("category", "string"),
        schema("userData", "string"));
    verifyDataRows(result, rows("1", "2", "3", "simple", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testSpathWithFieldAndWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | fields c, * | head"
                + " 1");
    verifySchema(
        result,
        schema("c", "string"),
        schema("a", "string"),
        schema("b", "string"),
        schema("category", "string"),
        schema("userData", "string"));
    verifyDataRows(result, rows("3", "1", "2", "simple", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testSpathWithFieldAndWildcardAtMiddle() throws IOException {
    verifyExplainException(
        "source=test_json | where category='simple' | spath input=userData | fields c, *, b",
        "Wildcard can be placed only at the end of the fields list (limit of spath command).");
  }

  @Test
  public void testSpathTypes() throws IOException {
    JSONObject result =
        executeQuery("source=test_json | where category='types' | spath input=userData | head 1");
    verifySchema(
        result,
        schema("boolean", "string"),
        schema("category", "string"),
        schema("null", "string"),
        schema("number", "string"),
        schema("string", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(
            "true",
            "types",
            null,
            "10.1",
            "STRING",
            sj("{'string': 'STRING', 'boolean': true, 'number': 10.1, 'null': null}")));
  }

  private static final String EXPECTED_SUBQUERY_ERROR =
      "Filter by subquery is not supported with field resolution.";

  @Test
  public void testSpathWithSubsearch() throws IOException {
    verifyExplainException(
        "source=test_json | spath input=userData | where b in [source=test_json | fields a] |"
            + " fields b",
        EXPECTED_SUBQUERY_ERROR);
  }

  @Test
  public void testSpathWithFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | fields a, b, c |"
                + " head 1");
    verifySchema(result, schema("a", "string"), schema("b", "string"), schema("c", "string"));
    verifyDataRows(result, rows("1", "2", "3"));
  }

  @Test
  public void testSpathWithAbsentField() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | fields a, x | head"
                + " 1");
    verifySchema(result, schema("a", "string"), schema("x", "string"));
    verifyDataRows(result, rows("1", null));
  }

  @Test
  public void testOverwrap() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='overwrap' | spath input=userData | fields a.b |"
                + " head 1");
    verifySchema(result, schema("a.b", "string"));
    verifyDataRows(result, rows("[1, 2]"));
  }

  @Test
  public void testSpathTwice() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | spath"
                + " input=userData | fields a, userData | head 1");
    verifySchema(result, schema("a", "string"), schema("userData", "string"));
    verifyDataRows(result, rows("[1, 1]", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testSpathTwiceWithDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | spath"
                + " input=userData | fields b, * | head 1");
    verifySchema(
        result,
        schema("b", "string"),
        schema("a", "string"),
        schema("c", "string"),
        schema("category", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result, rows("[2, 2]", "[1, 1]", "[3, 3]", "simple", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testSpathWithEval() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData |"
                + " eval result = a * b * c | fields result | head 1");
    verifySchema(result, schema("result", "double"));
    verifyDataRows(result, rows(6));
  }

  @Test
  public void testSpathWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData |"
                + "stats count by a, b | head 1");
    verifySchema(result, schema("count", "bigint"), schema("a", "string"), schema("b", "string"));
    verifyDataRows(result, rows(2, "1", "2"));
  }

  @Test
  public void testSpathWithNestedFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='nested' | spath input=userData | fields"
                + " `nested.d{}`, nested.e");
    verifySchema(result, schema("nested.d{}", "string"), schema("nested.e", "string"));
    verifyDataRows(result, rows("[1, 2, 3]", "str"));
  }

  @Test
  public void testAppendWithSpathInMainAndDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | head 1 | append"
                + " [source=test_json | where category='simple' | eval d = 4 | head 1 ] | fields"
                + " a, c, *");
    verifySchema(
        result,
        schema("a", "string"),
        schema("c", "string"),
        schema("b", "string"),
        schema("category", "string"),
        schema("d", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows("1", "3", "2", "simple", null, sj("{'a': 1, 'b': 2, 'c': 3}")),
        rows(null, null, null, "simple", "4", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testAppendWithSpathInSubsearchDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | eval d = 4 | head 1 | append"
                + " [source=test_json | where category='simple' | spath input=userData | head 1 ] |"
                + " fields a, c, *");
    verifySchema(
        result,
        schema("a", "string"),
        schema("c", "string"),
        schema("b", "string"),
        schema("category", "string"),
        schema("d", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(null, null, null, "simple", "4", sj("{'a': 1, 'b': 2, 'c': 3}")),
        rows("1", "3", "2", "simple", null, sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testAppendColWithSpathInMain() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | appendcol [where"
                + " category='simple'] | fields a, c, *");
    verifySchema(
        result,
        schema("a", "string"),
        schema("c", "string"),
        schema("category", "string"),
        schema("userData", "string"),
        schema("b", "string"));
    verifyDataRows(
        result,
        rows("1", "3", "simple", sj("{'a': 1, 'b': 2, 'c': 3}"), "2"),
        rows("1", "3", "simple", sj("{'a': 1, 'b': 2, 'c': 3}"), "2"));
  }

  @Test
  public void testAppendColWithSpathInSubsearch() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | appendcol [where category='simple' |"
                + " spath input=userData] | fields a, c, *");
    verifySchema(
        result,
        schema("a", "string"),
        schema("c", "string"),
        schema("category", "string"),
        schema("userData", "string"),
        schema("b", "string"));
    verifyDataRows(
        result,
        rows("1", "3", "simple", sj("{'a': 1, 'b': 2, 'c': 3}"), "2"),
        rows("1", "3", "simple", sj("{'a': 1, 'b': 2, 'c': 3}"), "2"));
  }

  @Test
  public void testAppendColWithSpathInBothInputs() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | appendcol [where"
                + " category='simple' | spath input=userData ] | fields a, c, *");
    verifySchema(
        result,
        schema("a", "string"),
        schema("c", "string"),
        schema("b", "string"),
        schema("category", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows("1", "3", "2", "simple", sj("{'a': 1, 'b': 2, 'c': 3}")),
        rows("1", "3", "2", "simple", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testAppendPipeWithSpathInMain() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' | spath input=userData | stats sum(a) as"
                + " total by b | appendpipe [stats sum(total) as total] | head 5");
    verifySchema(result, schema("total", "double"), schema("b", "string"));
    verifyDataRows(result, rows(2, "2"), rows(2, null));
  }

  @Test
  public void testMultisearchWithSpath() throws IOException {
    JSONObject result =
        executeQuery(
            "| multisearch [source=test_json | where category='simple' | spath input=userData |"
                + " head 1] [source=test_json | where category='nested' | spath input=userData] |"
                + " fields a, c, *");
    verifySchema(
        result,
        schema("a", "string"),
        schema("c", "string"),
        schema("b", "string"),
        schema("category", "string"),
        schema("nested.d{}", "string"),
        schema("nested.e", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows("1", "3", "2", "simple", null, null, sj("{'a': 1, 'b': 2, 'c': 3}")),
        rows(
            null,
            null,
            null,
            "nested",
            "[1, 2, 3]",
            "str",
            sj("{'nested': {'d': [1, 2, 3], 'e': 'str'}}")));
  }

  @Test
  public void testSpathWithMvCombine() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' "
                + "| spath input=userData "
                + "| fields a, b, c "
                + "| mvcombine c");

    verifySchema(result, schema("a", "string"), schema("b", "string"), schema("c", "array"));

    verifyDataRows(result, rows("1", "2", new String[] {"3", "3"}));
  }
}
