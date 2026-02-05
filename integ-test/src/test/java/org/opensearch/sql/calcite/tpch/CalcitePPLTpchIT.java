/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tpch;

import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.Retry;

@Retry
public class CalcitePPLTpchIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.TPCH_CUSTOMER);
    loadIndex(Index.TPCH_LINEITEM);
    loadIndex(Index.TPCH_ORDERS);
    loadIndex(Index.TPCH_SUPPLIER);
    loadIndex(Index.TPCH_PART);
    loadIndex(Index.TPCH_PARTSUPP);
    loadIndex(Index.TPCH_NATION);
    loadIndex(Index.TPCH_REGION);
  }

  @Ignore("Flaky test - see https://github.com/opensearch-project/sql/issues/XXXX")
  @Test
  public void testQ1() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q1.ppl"));
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
        schema("count_order", "bigint"));
    verifyDataRows(
        actual,
        closeTo(
            "A",
            "F",
            37474,
            isPushdownDisabled() ? 37569624.63999998 : 37569624.64,
            isPushdownDisabled() ? 35676192.096999995 : 35676192.097,
            isPushdownDisabled() ? 37101416.22242404 : 37101416.222424,
            25.354533152909337,
            isPushdownDisabled() ? 25419.231826792948 : 25419.231826792962,
            isPushdownDisabled() ? 0.050866035182679493 : 0.0508660351826793,
            1478),
        closeTo(
            "N",
            "F",
            1041,
            1041301.07,
            isPushdownDisabled() ? 999060.8979999998 : 999060.898,
            isPushdownDisabled() ? 1036450.80228 : 1036450.8022800001,
            27.394736842105264,
            27402.659736842103,
            isPushdownDisabled() ? 0.042894736842105284 : 0.04289473684210526,
            38),
        closeTo(
            "N",
            "O",
            75168,
            isPushdownDisabled() ? 75384955.36999969 : 75384955.37,
            isPushdownDisabled() ? 71653166.30340016 : 71653166.3034,
            isPushdownDisabled() ? 74498798.13307281 : 74498798.133073,
            25.558653519211152,
            isPushdownDisabled() ? 25632.422771166166 : 25632.42277116627,
            isPushdownDisabled() ? 0.04969738184291069 : 0.049697381842910573,
            2941),
        closeTo(
            "R",
            "F",
            36511,
            36570841.24,
            isPushdownDisabled() ? 34738472.87580004 : 34738472.8758,
            isPushdownDisabled() ? 36169060.11219294 : 36169060.112193,
            25.059025394646532,
            25100.09693891558,
            isPushdownDisabled() ? 0.050027453671928686 : 0.05002745367192862,
            1457));
  }

  @Test
  public void testQ2() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q2.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("s_acctbal", "double"),
        schema("s_name", "string"),
        schema("n_name", "string"),
        schema("p_partkey", "bigint"),
        schema("p_mfgr", "string"),
        schema("s_address", "string"),
        schema("s_phone", "string"),
        schema("s_comment", "string"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ3() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q3.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("l_orderkey", "bigint"),
        schema("revenue", "double"),
        schema("o_orderdate", "timestamp"),
        schema("o_shippriority", "int"));
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
  public void testQ4() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q4.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual, schema("o_orderpriority", "string"), schema("order_count", "bigint"));
    verifyDataRows(
        actual,
        rows("1-URGENT", 9),
        rows("2-HIGH", 7),
        rows("3-MEDIUM", 9),
        rows("4-NOT SPECIFIED", 8),
        rows("5-LOW", 12));
  }

  @Test
  public void testQ5() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q5.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("n_name", "string"), schema("revenue", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ6() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q6.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("revenue", "double"));
    verifyDataRows(actual, rows(77949.9186));
  }

  @Test
  public void testQ7() throws IOException {
    String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
    Assume.assumeFalse("testQ7 on macOS CI could socket timeout", osName.contains("mac"));
    String ppl = sanitize(loadFromFile("tpch/queries/q7.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("supp_nation", "string"),
        schema("cust_nation", "string"),
        schema("l_year", "int"),
        schema("revenue", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ8() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q8.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("o_year", "int"), schema("mkt_share", "double"));
    verifyDataRows(actual, rows(1995, 0.0), rows(1996, 0.0));
  }

  @Test
  public void testQ9() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q9.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("nation", "string"),
        schema("o_year", "int"),
        schema("sum_profit", "double"));
    verifyNumOfRows(actual, 60);
  }

  @Test
  public void testQ10() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q10.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("c_custkey", "bigint"),
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
  public void testQ11() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q11.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("ps_partkey", "bigint"), schema("value", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ12() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q12.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("l_shipmode", "string"),
        schema("high_line_count", "bigint"),
        schema("low_line_count", "bigint"));
    verifyDataRows(actual, rows("MAIL", 5, 5), rows("SHIP", 5, 10));
  }

  @Test
  public void testQ13() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q13.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("c_count", "bigint"), schema("custdist", "bigint"));
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
  public void testQ14() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q14.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("promo_revenue", "double"));
    verifyDataRows(actual, closeTo(15.230212611597254));
  }

  @Test
  public void testQ15() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q15.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("s_suppkey", "bigint"),
        schema("s_name", "string"),
        schema("s_address", "string"),
        schema("s_phone", "string"),
        schema("total_revenue", "double"));
    verifyDataRows(
        actual,
        rows(10, "Supplier#000000010", "Saygah3gYWMp72i PY", "34-852-489-8585", 797313.3838));
  }

  @Test
  public void testQ16() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q16.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("p_brand", "string"),
        schema("p_type", "string"),
        schema("p_size", "int"),
        schema("supplier_cnt", "bigint"));
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
  public void testQ17() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q17.ppl"));
    String actual = executeQuery(ppl).toString();
    assertJsonEquals(
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
  public void testQ18() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q18.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("c_name", "string"),
        schema("c_custkey", "bigint"),
        schema("o_orderkey", "bigint"),
        schema("o_orderdate", "timestamp"),
        schema("o_totalprice", "double"),
        schema("sum(l_quantity)", "double"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ19() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q19.ppl"));
    String actual = executeQuery(ppl).toString();
    assertJsonEquals(
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
  public void testQ20() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q20.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("s_name", "string"), schema("s_address", "string"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ21() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q21.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(actual, schema("s_name", "string"), schema("numwait", "bigint"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testQ22() throws IOException {
    String ppl = sanitize(loadFromFile("tpch/queries/q22.ppl"));
    JSONObject actual = executeQuery(ppl);
    verifySchemaInOrder(
        actual,
        schema("cntrycode", "string"),
        schema("numcust", "bigint"),
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
