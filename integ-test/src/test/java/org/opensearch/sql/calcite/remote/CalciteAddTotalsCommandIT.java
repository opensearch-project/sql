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

public class CalciteAddTotalsCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  /**
   * default test without parameters on account index
   *
   * @throws IOException
   */
  @Test
  public void testAddTotalsTotalWithTotalField() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields age, balance | addtotals",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result, schema("age", "bigint"), schema("balance", "bigint"), schema("Total", "bigint"));

    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");
    // Iterate through all data rows
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);

      BigDecimal cRowTotal = new BigDecimal(0);
      // Iterate through each field in the row
      for (int j = 0; j < row.length() - 1; j++) {
        Object value = row.isNull(j) ? 0 : row.get(j);
        if (value instanceof Integer) {
          cRowTotal = cRowTotal.add(new BigDecimal((Integer) (value)));
        } else if (value instanceof Double) {
          cRowTotal = cRowTotal.add(new BigDecimal((Double) (value)));
        } else if (value instanceof String) {
          cRowTotal = cRowTotal.add(new BigDecimal((String) (value)));
        }
      }
      BigDecimal foundTotal = row.getBigDecimal(row.length() - 1);
      assertEquals(foundTotal.doubleValue(), cRowTotal.doubleValue(), 0.000001);
    }
  }

  @Test
  public void testAddTotalsRowWithSpecificFields() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields age, balance | addtotals balance",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result, schema("age", "bigint"), schema("balance", "bigint"), schema("Total", "bigint"));

    //  sum for balance, "Total" for label
    var dataRows = result.getJSONArray("datarows");
    // Iterate through all data rows
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);

      BigDecimal cRowTotal = new BigDecimal(0);
      // Iterate through each field in the row

      Object value = row.isNull(1) ? 0 : row.get(1);
      if (value instanceof Integer) {
        cRowTotal = cRowTotal.add(new BigDecimal((Integer) (value)));
      } else if (value instanceof Double) {
        cRowTotal = cRowTotal.add(new BigDecimal((Double) (value)));
      } else if (value instanceof String) {
        cRowTotal = cRowTotal.add(new BigDecimal((String) (value)));
      }

      BigDecimal foundTotal = row.getBigDecimal(row.length() - 1);
      assertEquals(foundTotal.doubleValue(), cRowTotal.doubleValue(), 0.000001);
    }
  }

  public static boolean isNumeric(String str) {
    return str != null && str.matches("-?\\d+(\\.\\d+)?");
  }

  private void compareDataRowTotals(
      org.json.JSONArray dataRows, List<Integer> fieldIndexes, int totalColIndex) {
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);

      BigDecimal cRowTotal = new BigDecimal(0);
      // Iterate through each field in the row
      for (int j = 0; j < fieldIndexes.size(); j++) {
        int colIndex = fieldIndexes.get(j);
        Object value = row.isNull(colIndex) ? 0 : row.get(colIndex);
        if (value instanceof Integer) {
          cRowTotal = cRowTotal.add(new BigDecimal((Integer) (value)));
        } else if (value instanceof Double) {
          cRowTotal = cRowTotal.add(new BigDecimal((Double) (value)));
        } else if (value instanceof BigDecimal) {
          cRowTotal = cRowTotal.add((BigDecimal) value);

        } else if (value instanceof String) {
          if (isNumeric((String) value)) {
            cRowTotal = cRowTotal.add(new BigDecimal((String) (value)));
          }
        }
      }
      BigDecimal foundTotal = row.getBigDecimal(totalColIndex);
      assertEquals(foundTotal.doubleValue(), cRowTotal.doubleValue(), 0.000001);
    }
  }

  private void verifyColTotals(
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
          if (isNumeric((String) value)) {
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
    String foundSummaryEventLabel = total_row.getString(total_row.length() - 1);
    assertEquals(foundSummaryEventLabel, finalSummaryEventLevel);
  }

  @Test
  public void testAddTotalsRowFieldsNonNumeric() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 |fields age address balance  | addtotals ",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result,
        schema("age", "bigint"),
        schema("address", "string"),
        schema("balance", "bigint"),
        schema("Total", "bigint"));

    //  sum for balance, "Total" for label
    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");
    // Iterate through all data rows
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);

      BigDecimal cRowTotal = new BigDecimal(0);
      // Iterate through each field in the row
      for (int j = 0; j < row.length() - 1; j++) {
        Object value = row.isNull(j) ? 0 : row.get(j);
        if (value instanceof Integer) {
          cRowTotal = cRowTotal.add(new BigDecimal((Integer) (value)));
        } else if (value instanceof Double) {
          cRowTotal = cRowTotal.add(new BigDecimal((Double) (value)));
        } else if (value instanceof String) {
          if (org.opensearch.sql.calcite.remote.CalciteAddTotalsCommandIT.isNumeric(
              (String) value)) {
            cRowTotal = cRowTotal.add(new BigDecimal((String) (value)));
          }
        }
      }
      BigDecimal foundTotal = row.getBigDecimal(row.length() - 1);
      assertEquals(foundTotal.doubleValue(), cRowTotal.doubleValue(), 0.000001);
    }
  }

  @Test
  public void testAddTotalsWithCustomLabel() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | head 2|fields age, balance | addtotals"
                    + " fieldname='Grand Total'",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("Grand Total", "bigint"));
  }

  @Test
  public void testAddTotalsAfterStats() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | stats count() by gender | addtotals `count()`", TEST_INDEX_ACCOUNT));

    var dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);
      assertEquals(row.get(0), row.get(2));
    }
  }

  @Test
  public void testAddTotalsWithNoData() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 1000 | fields age, balance | addtotals",
                TEST_INDEX_ACCOUNT));

    // Should still have totals row even with no input data
    var dataRows = result.getJSONArray("datarows");
    assertEquals(0, dataRows.length()); // Only totals row
  }

  @Test
  public void testAddTotalsInComplexPipeline() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | stats avg(balance) as avg_balance, count() as"
                    + " total_count by gender | addtotals avg_balance, total_count",
                TEST_INDEX_ACCOUNT));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(0);
    field_indexes.add(1);

    compareDataRowTotals(dataRows, field_indexes, 3);
  }

  @Test
  public void testAddTotalsWithRowFalse() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields age, balance | addtotals row=false",
                TEST_INDEX_ACCOUNT));

    // With row=false, should not append totals row
    var dataRows = result.getJSONArray("datarows");

    // Verify that no totals row was added - all rows should have actual data
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);
      // None of these rows should have "Total" label
      for (int j = 0; j < row.length(); j++) {
        if (!row.isNull(j) && row.get(j).equals("Total")) {
          fail("Found totals row when row=false was specified");
        }
      }
    }
  }

  @Test
  public void testAddTotalsWithLabelAndLabelField() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 |head 3| fields age, balance | addtotals row=false"
                    + " col=true label='Sum'  labelfield='Total Summary'",
                TEST_INDEX_ACCOUNT));

    // Verify schema includes custom fieldname
    verifySchema(
        result,
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("Total Summary", "string"));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(0);
    field_indexes.add(1);

    verifyColTotals(dataRows, field_indexes, "Sum");
  }

  @Test
  public void testAddTotalsWithFieldnameAndSpecificFields() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 |head 2| fields age, balance | addtotals balance"
                    + " fieldname='BalanceSum'",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("BalanceSum", "bigint"));

    var dataRows = result.getJSONArray("datarows");
    ArrayList<Integer> field_indexes = new ArrayList<>();
    field_indexes.add(1);

    compareDataRowTotals(dataRows, field_indexes, 2);
  }

  @Test
  public void testAddTotalsWithFieldnameNoRow() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields age, balance | "
                    + "addtotals balance fieldname='CustomSum' row=false",
                TEST_INDEX_ACCOUNT));

    // With row=false, should not append totals row regardless of fieldname
    var dataRows = result.getJSONArray("datarows");

    // Verify that no totals row was added
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);
      // None of these rows should have "CustomSum" label
      for (int j = 0; j < row.length(); j++) {
        if (!row.isNull(j) && row.get(j).equals("CustomSum")) {
          fail("Found totals row when row=false was specified");
        }
      }
    }
  }
}
