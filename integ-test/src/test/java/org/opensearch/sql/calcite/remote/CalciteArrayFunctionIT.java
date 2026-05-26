/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteArrayFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
    loadIndex(Index.ARRAY);
  }

  @Test
  public void testArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1.5, 2, 1.0) | head 1 | fields array",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("array", "array"));

    verifyDataRows(actual, rows(List.of(1, -1.5, 2, 1.0)));
  }

  @Test
  public void testArrayWithString() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 'demo') | head 1 | fields array",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("array", "array"));

    verifyDataRows(actual, rows(List.of("1", "demo")));
  }

  @Test
  public void testArrayWithMix() {
    Class<? extends Exception> expectedException =
        isStandaloneTest() ? RuntimeException.class : ResponseException.class;
    Exception e =
        assertThrows(
            expectedException,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval array = array(1, true) | head 1 | fields array",
                        TEST_INDEX_BANK)));

    verifyErrorMessageContains(
        e,
        "fail to create array with fixed type: At line 0, column 0: Cannot infer return type for"
            + " array; operand types: [INTEGER, BOOLEAN]");
  }

  @Test
  public void testArrayLength() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1.5, 2, 1.0) | eval length ="
                    + " array_length(array) | head 1 | fields length",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("length", "int"));

    verifyDataRows(actual, rows(4));
  }

  @Test
  public void testForAll() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = forall(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "boolean"));

    verifyDataRows(actual, rows(false));
  }

  @Test
  public void testExists() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = exists(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "boolean"));

    verifyDataRows(actual, rows(true));
  }

  @Test
  public void testFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = filter(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 2)));
  }

  @Test
  public void testTransform() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = transform(array, x -> x + 1) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(2, 3, 4)));
  }

  @Test
  public void testTransformForTwoInput() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = transform(array, (x, i) -> x +"
                    + " i) | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 3, 5)));
  }

  @Test
  public void testTransformForWithDouble() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = transform(array, (x, i) -> x +"
                    + " i * 10.1) | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 12.1, 23.2)));
  }

  @Test
  public void testTransformForWithUDF() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(TIMESTAMP('2000-01-02 00:00:00'),"
                    + " TIMESTAMP('2000-01-03 00:00:00'), TIMESTAMP('2000-01-04 00:00:00')), result"
                    + " = transform(array, (x, i) -> DATEDIFF(x, TIMESTAMP('2000-01-01 23:59:59'))"
                    + " + i * 10.1) | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 12.1, 23.2)));
  }

  @Test
  public void testReduce() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc"
                    + " + x), result2 = reduce(array, 10, (acc, x) -> acc + x), result3 ="
                    + " reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10.0) | fields"
                    + " result,result2, result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(
        actual, schema("result", "int"), schema("result2", "int"), schema("result3", "double"));

    verifyDataRows(actual, rows(6, 16, 60));
  }

  @Test
  public void testReduce2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1.0, 2.0, 3.0), result3 = reduce(array, 0, (acc, x)"
                    + " -> acc * 10.0 + x, acc -> acc * 10.0) | fields result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result3", "double"));

    verifyDataRows(actual, rows(1230));
  }

  @Test
  public void testReduce3() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age=28 | eval array = array(1.0, 2.0, 3.0), result3 ="
                    + " reduce(array, age, (acc, x) -> acc * 1.0 + x, acc -> acc * 10.0) | fields"
                    + " result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result3", "double"));

    verifyDataRows(actual, rows(340));
  }

  @Test
  public void testReduceWithUDF() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array('a', 'ab', 'abc'), result3 = reduce(array, 0, (acc,"
                    + " x) -> acc + length(x), acc -> acc * 10.0) | fields result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result3", "double"));

    verifyDataRows(actual, rows(60));
  }

  @Test
  public void testMvjoinWithStringArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('a', 'b', 'c'), ',') | fields result | head"
                    + " 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("a,b,c"));
  }

  @Test
  public void testMvjoinWithStringifiedNumbers() throws IOException {
    // Note: mvjoin only supports string arrays
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('1', '2', '3'), ' | ') | fields result |"
                    + " head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("1 | 2 | 3"));
  }

  @Test
  public void testMvjoinWithMixedStringValues() throws IOException {
    // mvjoin only supports string arrays
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('1', 'text', '2.5'), ';') | fields result |"
                    + " head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("1;text;2.5"));
  }

  @Test
  public void testMvjoinWithEmptyArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array(), '-') | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows(""));
  }

  @Test
  public void testMvjoinWithStringBooleans() throws IOException {
    // mvjoin only supports string arrays
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('true', 'false', 'true'), '|') | fields"
                    + " result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("true|false|true"));
  }

  @Test
  public void testMvjoinWithSpecialDelimiters() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('apple', 'banana', 'cherry'), ' AND ') |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("apple AND banana AND cherry"));
  }

  @Test
  public void testMvjoinWithArrayFromRealFields() throws IOException {
    // Test mvjoin on arrays created from real fields using array() function
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval names_array = array(firstname, lastname) | eval result ="
                    + " mvjoin(names_array, ',') | fields firstname, lastname, result | head 1",
                TEST_INDEX_BANK));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("result", "string"));
    // Verify that mvjoin correctly joins the firstname and lastname fields
    JSONArray dataRows = actual.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    JSONArray firstRow = dataRows.getJSONArray(0);
    assertEquals(firstRow.getString(0) + "," + firstRow.getString(1), firstRow.getString(2));
  }

  @Test
  public void testMvjoinWithMultipleRealFields() throws IOException {
    // Test mvjoin with arrays created from multiple real fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval info_array = array(city, state, employer) | eval result ="
                    + " mvjoin(info_array, ' | ') | fields city, state, employer, result | head 1",
                TEST_INDEX_BANK));

    verifySchema(
        actual,
        schema("city", "string"),
        schema("state", "string"),
        schema("employer", "string"),
        schema("result", "string"));
    // Verify that mvjoin correctly joins the city, state, and employer fields
    JSONArray dataRows = actual.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    JSONArray firstRow = dataRows.getJSONArray(0);
    assertEquals(
        firstRow.getString(0) + " | " + firstRow.getString(1) + " | " + firstRow.getString(2),
        firstRow.getString(3));
  }

  @Test
  public void testMvindexSingleElementPositive() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('a', 'b', 'c', 'd', 'e'), result = mvindex(arr, 1)"
                    + " | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("b"));
  }

  @Test
  public void testMvindexSingleElementNegative() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('a', 'b', 'c', 'd', 'e'), result = mvindex(arr, -1)"
                    + " | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("e"));
  }

  @Test
  public void testMvindexSingleElementNegativeMiddle() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('a', 'b', 'c', 'd', 'e'), result = mvindex(arr, -3)"
                    + " | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("c"));
  }

  @Test
  public void testMvindexRangePositive() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, 1, 3) | head"
                    + " 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(2, 3, 4)));
  }

  @Test
  public void testMvindexRangeNegative() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, -3, -1) |"
                    + " head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(3, 4, 5)));
  }

  @Test
  public void testMvindexRangeMixed() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, -4, 2) | head"
                    + " 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(2, 3)));
  }

  @Test
  public void testMvindexRangeFirstThree() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('alex', 'celestino', 'claudia', 'david'), result ="
                    + " mvindex(arr, 0, 2) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("alex", "celestino", "claudia")));
  }

  @Test
  public void testMvindexRangeLastThree() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('buttercup', 'dash', 'flutter', 'honey', 'ivory',"
                    + " 'minty', 'pinky', 'rarity'), result = mvindex(arr, -3, -1) | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("minty", "pinky", "rarity")));
  }

  @Test
  public void testMvindexRangeSingleElement() throws IOException {
    // When start == end, should return single element in array
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, 2, 2) | head"
                    + " 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(3)));
  }

  @Test
  public void testMvfindWithMatch() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('apple', 'banana', 'apricot'), result = mvfind(arr,"
                    + " 'ban.*') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(1));
  }

  @Test
  public void testMvfindWithNoMatch() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('cat', 'dog', 'bird'), result = mvfind(arr, 'fish') |"
                    + " head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows((Object) null));
  }

  @Test
  public void testMvfindWithFirstMatch() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('error123', 'info', 'error456'), result ="
                    + " mvfind(arr, 'err.*') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(0));
  }

  @Test
  public void testMvfindWithMultipleMatches() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('test1', 'test2', 'test3'), result = mvfind(arr,"
                    + " 'test.*') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(0)); // Returns first match at index 0
  }

  @Test
  public void testMvfindWithComplexRegex() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('abc123', 'def456', 'ghi789'), result = mvfind(arr,"
                    + " 'def\\\\d+') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(1));
  }

  @Test
  public void testMvfindWithCaseInsensitive() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('Apple', 'Banana', 'Cherry'), result = mvfind(arr,"
                    + " '(?i)banana') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(1));
  }

  @Test
  public void testMvfindWithNumericArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(100, 200, 300), result = mvfind(arr, '200') | head 1"
                    + " | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(1));
  }

  @Test
  public void testMvfindWithEmptyArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(), result = mvfind(arr, 'test') | head 1 | fields"
                    + " result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows((Object) null));
  }

  @Test
  public void testMvfindWithDynamicRegex() throws IOException {
    // Test non-literal regex pattern (computed at runtime via concat)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('apple', 'banana', 'apricot'), pattern ="
                    + " concat('ban', '.*'), result = mvfind(arr, pattern) | head 1 | fields"
                    + " result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "int"));
    verifyDataRows(actual, rows(1));
  }

  @Test
  public void testMvzipBasic() throws IOException {
    // Basic example from spec: eval nserver=mvzip(hosts,ports)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval hosts = array('host1', 'host2'), ports = array(80, 443), nserver"
                    + " = mvzip(hosts, ports) | head 1 | fields nserver",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("nserver", "array"));
    verifyDataRows(actual, rows(List.of("host1,80", "host2,443")));
  }

  @Test
  public void testMvzipWithCustomDelimiter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr1 = array('a', 'b', 'c'), arr2 = array('x', 'y', 'z'), result"
                    + " = mvzip(arr1, arr2, '|') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("a|x", "b|y", "c|z")));
  }

  @Test
  public void testMvzipNested() throws IOException {
    // Example from spec: mvzip(mvzip(field1,field2,"|"),field3,"|")
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval field1 = array('a', 'b'), field2 = array('c', 'd'), field3 ="
                    + " array('e', 'f'), result = mvzip(mvzip(field1, field2, '|'), field3, '|') |"
                    + " head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("a|c|e", "b|d|f")));
  }

  @Test
  public void testMvzipWithEmptyArray() throws IOException {
    // When one array is empty, result should be empty array (not null)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvzip(array(), array('a', 'b')) | head 1 | fields"
                    + " result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of()));
  }

  @Test
  public void testMvzipWithBothEmptyArrays() throws IOException {
    // When both arrays are empty, result should be empty array (not null)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvzip(array(), array()) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of()));
  }

  @Test
  public void testMvdedupWithDuplicates() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 2, 3, 1, 4), result = mvdedup(arr) | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2, 3, 4)));
  }

  @Test
  public void testMvdedupWithNoDuplicates() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3, 4), result = mvdedup(arr) | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2, 3, 4)));
  }

  @Test
  public void testMvdedupWithAllDuplicates() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(5, 5, 5, 5), result = mvdedup(arr) | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(5)));
  }

  @Test
  public void testMvdedupWithEmptyArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(), result = mvdedup(arr) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of()));
  }

  @Test
  public void testMvdedupWithStrings() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('apple', 'banana', 'apple', 'cherry', 'banana'),"
                    + " result = mvdedup(arr) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("apple", "banana", "cherry")));
  }

  @Test
  public void testMvdedupPreservesOrder() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array('z', 'a', 'z', 'b', 'a', 'c'), result ="
                    + " mvdedup(arr) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    // Should preserve first occurrence order: z, a, b, c
    verifyDataRows(actual, rows(List.of("z", "a", "b", "c")));
  }

  @Test
  public void testSplitWithSemicolonDelimiter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval test = 'buttercup;rarity;tenderhoof;dash;mcintosh', result ="
                    + " split(test, ';') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("buttercup", "rarity", "tenderhoof", "dash", "mcintosh")));
  }

  @Test
  public void testSplitWithMultiCharDelimiter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval test = '1a2b3c4def567890', result = split(test, 'def') | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("1a2b3c4", "567890")));
  }

  @Test
  public void testSplitWithEmptyDelimiter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval test = 'abcd', result = split(test, '') | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    // Empty delimiter splits into individual characters
    verifyDataRows(actual, rows(List.of("a", "b", "c", "d")));
  }

  @Test
  public void testMvmap() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3), result = mvmap(arr, arr * 10) | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(10, 20, 30)));
  }

  @Test
  public void testMvmapWithAddition() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3), result = mvmap(arr, arr + 5) | head 1 |"
                    + " fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(6, 7, 8)));
  }

  @Test
  public void testMvmapWithNestedFunction() throws IOException {
    // Test mvmap with mvindex as first argument - extracts field name from nested function
    // Equivalent to Splunk: mvmap(mvindex(arr, 1, 3), arr * 10)
    // The lambda binds 'arr' and iterates over mvindex output (values at indices 1-3)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(10, 20, 30, 40, 50), result = mvmap(mvindex(arr, 1,"
                    + " 3), arr * 2) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    // mvindex(arr, 1, 3) returns [20, 30, 40], then mvmap multiplies each by 2
    verifyDataRows(actual, rows(List.of(40, 60, 80)));
  }

  @Test
  public void testMvmapWithOtherFieldReference() throws IOException {
    // Test mvmap with reference to another field in the expression
    // The first record in bank has age=32, so array(1,2,3) * 32 = [32, 64, 96]
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3), result = mvmap(arr, arr * age) | head 1 |"
                    + " fields age, result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("age", "int"), schema("result", "array"));
    verifyDataRows(actual, rows(32, List.of(32, 64, 96)));
  }

  @Test
  public void testMvmapWithEvalFieldReference() throws IOException {
    // Test mvmap with reference to another field created by eval
    // array(1,2,3) * 10 = [10, 20, 30]
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval arr = array(1, 2, 3), multiplier = 10, result = mvmap(arr, arr *"
                    + " multiplier) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(10, 20, 30)));
  }
}
