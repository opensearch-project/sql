/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tpch;

import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.calcite.standalone.CalcitePPLIntegTestCase;
import org.opensearch.sql.common.setting.Settings;

public class CalcitePPLTpchIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.TPCH_CUSTOMER);
    loadIndex(Index.TPCH_LINEITEM);
    loadIndex(Index.TPCH_ORDERS);
    loadIndex(Index.TPCH_SUPPLIER);
    loadIndex(Index.TPCH_PART);
    loadIndex(Index.TPCH_PARTSUPP);
    loadIndex(Index.TPCH_NATION);
    loadIndex(Index.TPCH_REGION);
  }

  @Override
  public Settings getSettings() {
    return new Settings() {
      private final Map<Key, Object> defaultSettings =
          new ImmutableMap.Builder<Key, Object>()
              .put(Key.QUERY_SIZE_LIMIT, 10000)
              .put(Key.FIELD_TYPE_TOLERANCE, true)
              .put(Key.CALCITE_ENGINE_ENABLED, true)
              .put(Key.CALCITE_FALLBACK_ALLOWED, false)
              .put(Key.CALCITE_PUSHDOWN_ENABLED, true)
              .put(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR, 0.9)
              .build();

      @Override
      public <T> T getSettingValue(Key key) {
        return (T) defaultSettings.get(key);
      }

      @Override
      public List<?> getSettings() {
        return (List<?>) defaultSettings;
      }
    };
  }

  String loadFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      return new String(Files.readAllBytes(Paths.get(uri)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testQ1() {
    String ppl = loadFromFile("tpch/queries/q1.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("l_returnflag", "string"),
        schema("l_linestatus", "string"),
        schema("sum_qty", "double"),
        schema("sum_base_price", "double"),
        schema("sum_disc_price", "double"),
        schema("sum_charge", "double"),
        schema("avg_qty", "double"),
        schema("avg_price", "double"),
        schema("avg_disc", "double"),
        schema("count_order", "long"));
    verifyDataRows(
        actual,
        rows(
            "A",
            "F",
            37474,
            37569624.63999998,
            35676192.096999995,
            37101416.22242404,
            25.354533152909337,
            25419.231826792948,
            0.050866035182679493,
            1478),
        rows(
            "N",
            "F",
            1041,
            1041301.07,
            999060.8979999998,
            1036450.80228,
            27.394736842105264,
            27402.659736842103,
            0.042894736842105284,
            38),
        rows(
            "N",
            "O",
            75168,
            75384955.36999969,
            71653166.30340016,
            74498798.13307281,
            25.558653519211152,
            25632.422771166166,
            0.04969738184291069,
            2941),
        rows(
            "R",
            "F",
            36511,
            36570841.24,
            34738472.87580004,
            36169060.11219294,
            25.059025394646532,
            25100.09693891558,
            0.050027453671928686,
            1457));
  }

  @Test
  public void testQ2() {
    String ppl = loadFromFile("tpch/queries/q2.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("s_acctbal", "double"),
        schema("s_name", "string"),
        schema("n_name", "string"),
        schema("p_partkey", "long"),
        schema("p_mfgr", "string"),
        schema("s_address", "string"),
        schema("s_phone", "string"),
        schema("s_comment", "string"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ3() {
    String ppl = loadFromFile("tpch/queries/q3.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("l_orderkey", "long"),
        schema("revenue", "double"),
        schema("o_orderdate", "timestamp"),
        schema("o_shippriority", "integer"));
    verifyDataRows(
        actual,
        rows(1637, 164224.9253, "1995-02-08 00:00:00", 0),
        rows(5191, 49378.309400000006, "1994-12-11 00:00:00", 0),
        rows(742, 43728.048, "1994-12-23 00:00:00", 0),
        rows(3492, 43716.072400000005, "1994-11-24 00:00:00", 0),
        rows(2883, 36666.9612, "1995-01-23 00:00:00", 0),
        rows(998, 11785.548600000002, "1994-11-26 00:00:00", 0),
        rows(3430, 4726.6775, "1994-12-12 00:00:00", 0),
        rows(4423, 3055.9365, "1995-02-17 00:00:00", 0));
  }

  @Test
  public void testQ4() {
    String ppl = loadFromFile("tpch/queries/q4.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("o_orderpriority", "string"), schema("order_count", "long"));
    verifyDataRows(
        actual,
        rows("1-URGENT", 9),
        rows("2-HIGH", 7),
        rows("3-MEDIUM", 9),
        rows("4-NOT SPECIFIED", 8),
        rows("5-LOW", 12));
  }

  @Test
  public void testQ5() {
    String ppl = loadFromFile("tpch/queries/q5.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("n_name", "string"), schema("revenue", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ6() {
    String ppl = loadFromFile("tpch/queries/q6.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("revenue", "double"));
    // TODO should be 77949.9186 when fix https://github.com/opensearch-project/sql/issues/3614
    verifyDataRows(actual, rows(48090.85860000001));
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3617")
  public void testQ7() {
    String ppl = loadFromFile("tpch/queries/q7.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("supp_nation", "string"),
        schema("cust_nation", "string"),
        schema("l_year", "integer"),
        schema("revenue", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3617")
  public void testQ8() {
    String ppl = loadFromFile("tpch/queries/q8.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("o_year", "integer"), schema("mkt_share", "double"));
    verifyDataRows(actual, rows(1995, 0.0), rows(1996, 0.0));
  }

  @Test
  public void testQ9() {
    String ppl = loadFromFile("tpch/queries/q9.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("nation", "string"),
        schema("o_year", "integer"),
        schema("sum_profit", "double"));
    verifyNumOfRows(actual, 60);
  }

  @Test
  public void testQ10() {
    String ppl = loadFromFile("tpch/queries/q10.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("c_custkey", "long"),
        schema("c_name", "string"),
        schema("revenue", "double"),
        schema("c_acctbal", "double"),
        schema("n_name", "string"),
        schema("c_address", "string"),
        schema("c_phone", "string"),
        schema("c_comment", "string"));
    verifyNumOfRows(actual, 20);
    actual = executeQuery(ppl + "| head 1");
    verifyDataRows(
        actual,
        rows(
            121,
            "Customer#000000121",
            282635.17189999996,
            6428.32,
            "PERU",
            "tv nCR2YKupGN73mQudO",
            "27-411-990-2959",
            "uriously stealthy ideas. carefully final courts use carefully"));
  }

  @Test
  public void testQ11() {
    String ppl = loadFromFile("tpch/queries/q11.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("ps_partkey", "long"), schema("value", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ12() {
    String ppl = loadFromFile("tpch/queries/q12.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("l_shipmode", "string"),
        schema("high_line_count", "integer"),
        schema("low_line_count", "integer"));
    verifyDataRows(actual, rows("MAIL", 5, 5), rows("SHIP", 5, 10));
  }

  @Test
  public void testQ13() {
    String ppl = loadFromFile("tpch/queries/q13.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("c_count", "long"), schema("custdist", "long"));
    verifyDataRows(
        actual,
        rows(0, 50),
        rows(16, 8),
        rows(17, 7),
        rows(20, 6),
        rows(13, 6),
        rows(12, 6),
        rows(9, 6),
        rows(23, 5),
        rows(14, 5),
        rows(10, 5),
        rows(21, 4),
        rows(18, 4),
        rows(11, 4),
        rows(8, 4),
        rows(7, 4),
        rows(26, 3),
        rows(22, 3),
        rows(6, 3),
        rows(5, 3),
        rows(4, 3),
        rows(29, 2),
        rows(24, 2),
        rows(19, 2),
        rows(15, 2),
        rows(28, 1),
        rows(25, 1),
        rows(3, 1));
  }

  @Test
  public void testQ14() {
    String ppl = loadFromFile("tpch/queries/q14.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("promo_revenue", "double"));
    verifyDataRows(actual, closeTo(15.230212611597254));
  }

  @Test
  public void testQ15() {
    String ppl = loadFromFile("tpch/queries/q15.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("s_suppkey", "long"),
        schema("s_name", "string"),
        schema("s_address", "string"),
        schema("s_phone", "string"),
        schema("total_revenue", "double"));
    verifyDataRows(
        actual,
        rows(10, "Supplier#000000010", "Saygah3gYWMp72i PY", "34-852-489-8585", 797313.3838));
  }

  @Test
  public void testQ16() {
    String ppl = loadFromFile("tpch/queries/q16.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("p_brand", "string"),
        schema("p_type", "string"),
        schema("p_size", "integer"),
        schema("supplier_cnt", "long"));
    verifyDataRows(
        actual,
        rows("Brand#11", "PROMO ANODIZED TIN", 45, 4),
        rows("Brand#11", "SMALL PLATED COPPER", 45, 4),
        rows("Brand#11", "STANDARD POLISHED TIN", 45, 4),
        rows("Brand#13", "MEDIUM ANODIZED STEEL", 36, 4),
        rows("Brand#14", "SMALL ANODIZED NICKEL", 45, 4),
        rows("Brand#15", "LARGE ANODIZED BRASS", 45, 4),
        rows("Brand#21", "LARGE BURNISHED COPPER", 19, 4),
        rows("Brand#23", "ECONOMY BRUSHED COPPER", 9, 4),
        rows("Brand#25", "MEDIUM PLATED BRASS", 45, 4),
        rows("Brand#31", "ECONOMY PLATED STEEL", 23, 4),
        rows("Brand#31", "PROMO POLISHED TIN", 23, 4),
        rows("Brand#32", "MEDIUM BURNISHED BRASS", 49, 4),
        rows("Brand#33", "LARGE BRUSHED TIN", 36, 4),
        rows("Brand#33", "SMALL BURNISHED NICKEL", 3, 4),
        rows("Brand#34", "LARGE PLATED BRASS", 45, 4),
        rows("Brand#34", "MEDIUM BRUSHED COPPER", 9, 4),
        rows("Brand#34", "SMALL PLATED BRASS", 14, 4),
        rows("Brand#35", "STANDARD ANODIZED STEEL", 23, 4),
        rows("Brand#43", "PROMO POLISHED BRASS", 19, 4),
        rows("Brand#43", "SMALL BRUSHED NICKEL", 9, 4),
        rows("Brand#44", "SMALL PLATED COPPER", 19, 4),
        rows("Brand#52", "MEDIUM BURNISHED TIN", 45, 4),
        rows("Brand#52", "SMALL BURNISHED NICKEL", 14, 4),
        rows("Brand#53", "MEDIUM BRUSHED COPPER", 3, 4),
        rows("Brand#55", "STANDARD ANODIZED BRASS", 36, 4),
        rows("Brand#55", "STANDARD BRUSHED COPPER", 3, 4),
        rows("Brand#13", "SMALL BRUSHED NICKEL", 19, 2),
        rows("Brand#25", "SMALL BURNISHED COPPER", 3, 2),
        rows("Brand#43", "MEDIUM ANODIZED BRASS", 14, 2),
        rows("Brand#53", "STANDARD PLATED STEEL", 45, 2),
        rows("Brand#24", "MEDIUM PLATED STEEL", 19, 1),
        rows("Brand#51", "ECONOMY POLISHED STEEL", 49, 1),
        rows("Brand#53", "LARGE BURNISHED NICKEL", 23, 1),
        rows("Brand#54", "ECONOMY ANODIZED BRASS", 9, 1));
  }

  @Test
  public void testQ17() {
    String ppl = loadFromFile("tpch/queries/q17.ppl");
    String actual = execute(ppl);
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"avg_yearly\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testQ18() {
    String ppl = loadFromFile("tpch/queries/q18.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("c_name", "string"),
        schema("c_custkey", "long"),
        schema("o_orderkey", "long"),
        schema("o_orderdate", "timestamp"),
        schema("o_totalprice", "double"),
        schema("sum(l_quantity)", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ19() {
    String ppl = loadFromFile("tpch/queries/q19.ppl");
    String actual = execute(ppl);
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"revenue\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testQ20() {
    String ppl = loadFromFile("tpch/queries/q20.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("s_name", "string"), schema("s_address", "string"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ21() {
    String ppl = loadFromFile("tpch/queries/q21.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("s_name", "string"), schema("numwait", "long"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ22() {
    String ppl = loadFromFile("tpch/queries/q22.ppl");
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("cntrycode", "string"),
        schema("numcust", "long"),
        schema("totacctbal", "double"));
    verifyDataRows(
        actual,
        rows("13", 1, 5679.84),
        rows("17", 1, 9127.27),
        rows("18", 2, 14647.99),
        rows("23", 1, 9255.67),
        rows("29", 2, 17195.08),
        rows("30", 1, 7638.57),
        rows("31", 1, 9331.13));
  }
}
