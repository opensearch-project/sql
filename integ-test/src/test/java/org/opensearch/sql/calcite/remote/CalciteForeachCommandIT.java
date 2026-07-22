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
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteForeachCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    if (!TestUtils.isIndexExist(client(), "test_foreach")) {
      String mapping =
          "{\"mappings\":{\"properties\":{"
              + "\"a\":{\"type\":\"long\"},"
              + "\"b\":{\"type\":\"long\"},"
              + "\"value_cpu\":{\"type\":\"long\"},"
              + "\"value_mem\":{\"type\":\"long\"}}}}";
      TestUtils.createIndexByRestClient(client(), "test_foreach", mapping);

      Request request1 = new Request("PUT", "/test_foreach/_doc/1?refresh=true");
      request1.setJsonEntity("{\"a\": 1, \"b\": 2, \"value_cpu\": 10, \"value_mem\": 20}");
      client().performRequest(request1);

      Request request2 = new Request("PUT", "/test_foreach/_doc/2?refresh=true");
      request2.setJsonEntity("{\"a\": 3, \"b\": 4, \"value_cpu\": 30, \"value_mem\": 40}");
      client().performRequest(request2);
    }
  }

  @Test
  public void testForeachExplicitFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | foreach a b [ eval <<FIELD>>_double = <<FIELD>> * 2 ] |"
                + " fields a_double, b_double");
    verifySchema(result, schema("a_double", "bigint"), schema("b_double", "bigint"));
    verifyDataRows(result, rows(2, 4), rows(6, 8));
  }

  @Test
  public void testForeachWildcardMatchstr() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | foreach value_* [ eval copy_<<MATCHSTR>> = <<FIELD>> ] |"
                + " fields copy_cpu, copy_mem");
    verifySchema(result, schema("copy_cpu", "bigint"), schema("copy_mem", "bigint"));
    verifyDataRows(result, rows(10, 20), rows(30, 40));
  }

  @Test
  public void testForeachCustomPlaceholders() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | foreach fieldstr=F matchstr=M value_* [ eval copy_<<M>> = F ] |"
                + " fields copy_cpu, copy_mem");
    verifySchema(result, schema("copy_cpu", "bigint"), schema("copy_mem", "bigint"));
    verifyDataRows(result, rows(10, 20), rows(30, 40));
  }

  @Test
  public void testForeachSubstitutesPlaceholderInsideStringLiteral() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | foreach a b [ eval <<FIELD>> = '<<FIELD>>' ] | fields a, b");
    verifySchema(result, schema("a", "string"), schema("b", "string"));
    verifyDataRows(result, rows("a", "b"), rows("a", "b"));
  }

  @Test
  public void testForeachMultivalueMode() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval nums = array(1, 2, 3), total = 0 | foreach mode=multivalue"
                + " itemstr=NUMBER nums [ eval total = total + NUMBER ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(6), rows(6));
  }

  @Test
  public void testForeachMultivalueIterPlaceholder() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval nums = array(10, 20, 30), total = 0 | foreach"
                + " mode=multivalue iterstr=IDX nums [ eval total = total + IDX ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(3), rows(3));
  }

  @Test
  public void testForeachAssignmentsUseUpdatedStateInOrder() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval nums=array(1,2), sum=0, seen=0 | foreach"
                + " mode=multivalue itemstr=ITEM nums [ eval sum=sum+ITEM, seen=seen+sum ] |"
                + " fields sum, seen");
    verifySchema(result, schema("sum", "int"), schema("seen", "int"));
    verifyDataRows(result, rows(3, 4), rows(3, 4));
  }

  @Test
  public void testForeachStringItemWithNumericIter() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval word = split('ABCDE', ''), nums = array('1', '2', '3',"
                + " '4', '5'), word_and_num = array() | foreach word mode=multivalue [ eval"
                + " word_and_num = mvappend(word_and_num, concat(<<ITEM>>, mvindex(nums,"
                + " <<ITER>>))) ] | fields word_and_num");
    verifySchema(result, schema("word_and_num", "array"));
    verifyDataRows(
        result,
        rows(List.of("A1", "B2", "C3", "D4", "E5")),
        rows(List.of("A1", "B2", "C3", "D4", "E5")));
  }

  @Test
  public void testForeachJsonArrayMode() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval total = 0 | foreach mode=json_array '[1,2,3]' [ eval total"
                + " = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "double"));
    verifyDataRows(result, rows(6.0), rows(6.0));
  }

  @Test
  public void testForeachJsonArrayFunctionWithoutMode() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval total = 0 | foreach json_array(1, 2, 3) [ eval total ="
                + " total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "double"));
    verifyDataRows(result, rows(6.0), rows(6.0));
  }

  @Test
  public void testForeachJsonArrayStringElements() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval result = '' | foreach json_array('a', 'b', 'c') [ eval"
                + " result = concat(result, <<ITEM>>) ] | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("abc"), rows("abc"));
  }

  @Test
  public void testForeachAutoCollectionsMode() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval nums = array(1, 2, 3), total = 0 | foreach"
                + " mode=auto_collections nums [ eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(6), rows(6));
  }

  @Test
  public void testForeachAutoCollectionsWithoutTarget() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval nums = array(1, 2, 3), total = 0 | foreach"
                + " mode=auto_collections [ eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(6), rows(6));
  }

  @Test
  public void testCollectionModeMismatchIsNoOp() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | eval nums=array(1,2), first=0, second=0 | foreach"
                + " mode=json_array nums [ eval first=first+<<ITEM>> ] | foreach"
                + " mode=multivalue '[1,2]' [ eval second=second+<<ITEM>> ] | fields first,"
                + " second");
    verifySchema(result, schema("first", "int"), schema("second", "int"));
    verifyDataRows(result, rows(0, 0), rows(0, 0));
  }
}
