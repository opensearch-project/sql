/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteAddColTotalsCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  @Test
  public void testAddColTotalsTotalWithTotalField() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields age, balance | addcoltotals",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(result, schema("age", "bigint"), schema("balance", "bigint"));

    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");
    // Iterate through all data rows
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(0);
    field_indexes.add(1);

    verifyColTotals(dataRows, field_indexes, null);
  }

  @Test
  public void testAddColTotalsRowWithSpecificFields() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields age, balance | addcoltotals balance",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(result, schema("age", "bigint"), schema("balance", "bigint"));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(1);

    verifyColTotals(dataRows, field_indexes, null);
  }

  public static boolean isNumeric(String str) {
    return str != null && str.matches("-?\\d+(\\.\\d+)?");
  }

  public void verifyColTotals(
      org.json.JSONArray dataRows, List<Integer> field_indexes, String finalSummaryEventLevel) {

    BigDecimal[] cColTotals = new BigDecimal[field_indexes.size()];
    for (int i = 0; i < dataRows.length() - 1; i++) {
      var row = dataRows.getJSONArray(i);

      // Iterate through each field in the row
      for (int j = 0; j < field_indexes.size(); j++) {

        int colIndex = field_indexes.get(j);
        if (cColTotals[j] == null) {
          cColTotals[j] = new BigDecimal(0);
        }
        Object value = row.isNull(colIndex) ? 0 : row.get(colIndex);
        if (value instanceof Integer) {
          cColTotals[j] = cColTotals[j].add(new BigDecimal((Integer) (value)));
        } else if (value instanceof Double) {
          cColTotals[j] = cColTotals[j].add(new BigDecimal((Double) (value)));
        } else if (value instanceof BigDecimal) {
          cColTotals[j] = cColTotals[j].add((BigDecimal) value);

        } else if (value instanceof String) {
          if (org.opensearch.sql.calcite.remote.CalciteAddColTotalsCommandIT.isNumeric(
              (String) value)) {
            cColTotals[j] = cColTotals[j].add(new BigDecimal((String) (value)));
          }
        }
      }
    }
    var total_row = dataRows.getJSONArray((dataRows.length() - 1));
    for (int j = 0; j < field_indexes.size(); j++) {
      int colIndex = field_indexes.get(j);
      BigDecimal foundTotal = total_row.getBigDecimal(colIndex);
      assertEquals(foundTotal.doubleValue(), cColTotals[j].doubleValue(), 0.000001);
    }
    if (finalSummaryEventLevel != null) {
      String foundSummaryEventLabel = total_row.getString(total_row.length() - 1);

      assertEquals(foundSummaryEventLabel, finalSummaryEventLevel);
    }
  }

  @Test
  public void testAddColTotalsRowFieldsNonNumeric() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 |fields age address balance  | addcoltotals ",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result, schema("age", "bigint"), schema("address", "string"), schema("balance", "bigint"));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(0);
    field_indexes.add(2);

    verifyColTotals(dataRows, field_indexes, null);
  }

  @Test
  public void testAddColTotalsWithCustomLabel() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | head 2|fields age, balance | addcoltotals label='Sum'"
                    + " labelfield='Grand Total'",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("Grand Total", "string"));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(0);
    field_indexes.add(1);

    verifyColTotals(dataRows, field_indexes, "Sum");
  }

  @Test
  public void testAddColTotalsWithNoData() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 1000 | fields age, balance | addcoltotals",
                TEST_INDEX_ACCOUNT));

    // Should still have totals row even with no input data
    var dataRows = result.getJSONArray("datarows");
    assertEquals(1, dataRows.length()); // Only totals row
  }

  @Test
  public void testAddColTotalsWithLabelAndLabelField() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 |head 3| fields age, balance,firstname | addcoltotals "
                    + " age balance  label='Sum'  labelfield='firstname'",
                TEST_INDEX_ACCOUNT));

    // Verify schema includes custom fieldname
    verifySchema(
        result,
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(0);
    field_indexes.add(1);

    verifyColTotals(dataRows, field_indexes, "Sum");
  }
}
