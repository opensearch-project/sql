/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.Capability.COMPARISON_DELEGATION;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

/** Integration tests for the PPL convert command with Calcite enabled. */
public class CalciteConvertCommandIT extends PPLIntegTestCase {
  private static final String WIDE_INDEX = "test_convert_wide";
  private static final int WIDE_ATTRIBUTES = 1_000;

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    enableCalcite();
  }

  @Test
  public void testConvertAutoFunction() throws IOException {
    // `head` without a preceding sort selects an undefined set of rows, so the exact values
    // asserted below only hold by accident of scan order. Sort on a unique key to fix which
    // rows are selected; the expected values are unchanged.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort account_number | convert auto(balance) | fields balance |"
                    + " head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyDataRows(result, rows(39225.0), rows(5686.0), rows(32838.0));
  }

  @Test
  public void testConvertAutoWithStringField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval test_field = '42' | convert auto(test_field) |"
                    + " fields test_field | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("test_field", null, "double"));
    verifyDataRows(result, rows(42.0));
  }

  @Test
  public void testConvertNumFunction() throws IOException {
    // `head` without a preceding sort selects an undefined set of rows, so the exact values
    // asserted below only hold by accident of scan order. Sort on a unique key to fix which
    // rows are selected; the expected values are unchanged.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort account_number | convert num(balance) | fields balance |"
                    + " head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyDataRows(result, rows(39225.0), rows(5686.0), rows(32838.0));
  }

  @Test
  public void testConvertWithAlias() throws IOException {
    // `head` without a preceding sort selects an undefined set of rows, so the exact values
    // asserted below only hold by accident of scan order. Sort on a unique key to fix which
    // rows are selected; the expected values are unchanged.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort account_number | convert auto(balance) AS balance_num |"
                    + " fields balance, balance_num | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "bigint"), schema("balance_num", null, "double"));
    verifyDataRows(result, rows(39225, 39225.0), rows(5686, 5686.0), rows(32838, 32838.0));
  }

  @Test
  public void testConvertMultipleFunctions() throws IOException {
    // `head` without a preceding sort selects an undefined set of rows, so the exact values
    // asserted below only hold by accident of scan order. Sort on a unique key to fix which
    // rows are selected; the expected values are unchanged.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort account_number | convert auto(balance), num(age) | fields"
                    + " balance, age | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"), schema("age", null, "double"));
    verifyDataRows(result, rows(39225.0, 32.0), rows(5686.0, 36.0), rows(32838.0, 28.0));
  }

  @Test
  public void testConvertRmcommaFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval amount = '1,234,567.89' | convert rmcomma(amount) |"
                    + " fields amount | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("amount", null, "double"));
    verifyDataRows(result, rows(1234567.89));
  }

  @Test
  public void testConvertRmunitFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval distance = '100km' | convert rmunit(distance) |"
                    + " fields distance | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("distance", null, "double"));
    verifyDataRows(result, rows(100.0));
  }

  @Test
  public void testConvertRmunitWithNoNumbers() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval duration = 'no numbers' | convert rmunit(duration) |"
                    + " fields duration | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("duration", null, "double"));
    verifyDataRows(result, rows((Object) null));
  }

  @Test
  public void testConvertMemkFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '100m' | convert memk(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(102400.0));
  }

  @Test
  public void testConvertMemkWithDefaultKilobytes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '100' | convert memk(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(100.0));
  }

  @Test
  public void testConvertMemkWithGigabytes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '2g' | convert memk(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(2097152.0));
  }

  @Test
  public void testConvertMemkWithNegative() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '-100m' | convert memk(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(-102400.0));
  }

  @Test
  public void testConvertNumWithNoNumbers() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval text = 'no numbers here' | convert num(text) |"
                    + " fields text | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("text", null, "double"));
    verifyDataRows(result, rows((Object) null));
  }

  @Test
  public void testConvertRmcommaWithLetters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval text = 'abc123' | convert rmcomma(text) |"
                    + " fields text | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("text", null, "double"));
    verifyDataRows(result, rows((Object) null));
  }

  @Test
  public void testConvertNoneFunction() throws IOException {
    // `head` without a preceding sort selects an undefined set of rows, so the exact values
    // asserted below only hold by accident of scan order. Sort on a unique key to fix which
    // rows are selected; the expected values are unchanged.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort account_number | convert none(account_number) | fields"
                    + " account_number | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", null, "bigint"));
    verifyDataRows(result, rows(1), rows(6), rows(13));
  }

  @Test
  public void testConvertWithWhere() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where age > 30 | convert auto(balance) | fields balance, age |"
                    + " head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"), schema("age", null, "int"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testConvertWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(age) | stats sum(age) by gender",
                TEST_INDEX_BANK));
    verifySchema(result, schema("sum(age)", null, "double"), schema("gender", "string"));
    verifyDataRows(result, rows(137.0, "M"), rows(101.0, "F"));
  }

  @Test
  public void testConvertAutoWithMemorySizes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '100m' | convert auto(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(102400.0));
  }

  @Test
  public void testConvertAutoWithMemorySizesKilobytes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '100k' | convert auto(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(100.0));
  }

  @Test
  public void testConvertAutoWithMemorySizesGigabytes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval memory = '2g' | convert auto(memory) |"
                    + " fields memory | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("memory", null, "double"));
    verifyDataRows(result, rows(2097152.0));
  }

  @Test
  public void testConvertMktimeWithDefaultFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval date_str = '10/18/2003 20:07:13' | convert"
                    + " mktime(date_str) | fields date_str | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("date_str", null, "double"));
    verifyDataRows(result, rows(1066507633.0));
  }

  @Test
  public void testConvertMktimeWithCustomTimeformat() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " | eval date_str = '18/10/2003 20:07:13' | convert timeformat=\\\"%d/%m/%Y"
                + " %H:%M:%S\\\" mktime(date_str) | fields date_str | head 1");
    verifySchema(result, schema("date_str", null, "double"));
    verifyDataRows(result, rows(1066507633.0));
  }

  @Test
  public void testConvertCtimeWithDefaultFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval timestamp = 1066507633 | convert ctime(timestamp) |"
                    + " fields timestamp | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("timestamp", null, "string"));
    verifyDataRows(result, rows("10/18/2003 20:07:13"));
  }

  @Test
  public void testConvertCtimeWithCustomTimeformat() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " | eval timestamp = 1066507633 | convert timeformat=\\\"%Y-%m-%d %H:%M:%S\\\""
                + " ctime(timestamp) | fields timestamp | head 1");
    verifySchema(result, schema("timestamp", null, "string"));
    verifyDataRows(result, rows("2003-10-18 20:07:13"));
  }

  @Test
  public void testConvertDur2secFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval duration = '01:23:45' | convert dur2sec(duration) |"
                    + " fields duration | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("duration", null, "double"));
    verifyDataRows(result, rows(5025.0));
  }

  @Test
  public void testConvertMstimeFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval time_str = '03:45' | convert mstime(time_str) |"
                    + " fields time_str | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("time_str", null, "double"));
    verifyDataRows(result, rows(225.0));
  }

  @Test
  public void testConvertTimeformatWithMultipleFunctions() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " | eval date_str = '18/10/2003 20:07:13', timestamp = 1066507633 | convert"
                + " timeformat=\\\"%d/%m/%Y %H:%M:%S\\\" mktime(date_str), ctime(timestamp) |"
                + " fields date_str, timestamp | head 1");
    verifySchema(result, schema("date_str", null, "double"), schema("timestamp", null, "string"));
    verifyNumOfRows(result, 1);
    assertEquals(1066507633.0, result.getJSONArray("datarows").getJSONArray(0).getDouble(0), 0.001);
    assertEquals(
        "18/10/2003 20:07:13", result.getJSONArray("datarows").getJSONArray(0).getString(1));
  }

  @Test
  @RequiresCapability(COMPARISON_DELEGATION)
  public void testConvertTimeformatWithWhere() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval date_str = '10/18/2003 20:07:13' |"
                    + " convert mktime(date_str) | where date_str > 1000000000 |"
                    + " fields date_str | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("date_str", null, "double"));
    verifyDataRows(result, rows(1066507633.0));
  }

  @Test
  public void testConvertTimeformatWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " | eval timestamp = 1066507633 |"
                + " convert timeformat=\\\"%Y\\\" ctime(timestamp) |"
                + " stats count() by timestamp");
    verifySchema(result, schema("count()", null, "bigint"), schema("timestamp", "string"));
    verifyDataRows(result, rows(7, "2003"));
  }

  /** Convert on an index with many dynamic attribute fields keeps every other column's value. */
  @Test
  public void testConvertOnWideIndex() throws IOException {
    createWideIndexIfAbsent();
    String last = "attributes.f" + (WIDE_ATTRIBUTES - 1);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | convert num(code), rmcomma(%s) AS last_num | fields code, status,"
                    + " attributes.f1, %s, last_num",
                WIDE_INDEX, last, last));
    verifySchema(
        result,
        schema("code", null, "double"),
        schema("status", null, "string"),
        schema("attributes.f1", null, "string"),
        schema(last, null, "string"),
        schema("last_num", null, "double"));
    verifyDataRows(result, rows(201.0, "s1", "v1", "1,234", 1234.0));
  }

  private void createWideIndexIfAbsent() throws IOException {
    if (TestUtils.isIndexExist(client(), WIDE_INDEX)) {
      return;
    }
    JSONObject attributes = new JSONObject();
    for (int i = 0; i < WIDE_ATTRIBUTES; i++) {
      attributes.put("f" + i, new JSONObject().put("type", "keyword"));
    }
    JSONObject properties =
        new JSONObject()
            .put("@timestamp", new JSONObject().put("type", "date"))
            .put("status", new JSONObject().put("type", "keyword"))
            .put("code", new JSONObject().put("type", "long"))
            .put("attributes", new JSONObject().put("properties", attributes));
    JSONObject fieldLimit =
        new JSONObject().put("total_fields", new JSONObject().put("limit", WIDE_ATTRIBUTES + 100));
    JSONObject body =
        new JSONObject()
            .put(
                "settings",
                new JSONObject().put("index", new JSONObject().put("mapping", fieldLimit)))
            .put("mappings", new JSONObject().put("properties", properties));
    TestUtils.createIndexByRestClient(client(), WIDE_INDEX, body.toString());

    Request doc = TestUtils.seedDocRequest(WIDE_INDEX, "1");
    doc.setJsonEntity(
        String.format(
            "{\"@timestamp\": \"2026-11-09T00:00:00Z\", \"status\": \"s1\", \"code\": 201,"
                + " \"attributes\": {\"f1\": \"v1\", \"f%d\": \"1,234\"}}",
            WIDE_ATTRIBUTES - 1));
    client().performRequest(doc);
  }
}
