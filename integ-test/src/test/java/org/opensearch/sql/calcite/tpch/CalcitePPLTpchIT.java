/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tpch;

import com.google.common.io.ByteStreams;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.calcite.standalone.CalcitePPLIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TPCH_CUSTOMER;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TPCH_LINEITEM;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

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

  private String resourceToString(String resource) {
    try (InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)) {
      byte[] data = ByteStreams.toByteArray(inStream);
      return new String(data, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testTpchQuery() {
    String actual =
        execute(
            String.format(
                "source=%s | where l_shipdate <= subdate(date('1998-12-01'), 90)",
                TEST_INDEX_TPCH_LINEITEM));
    assertEquals("", actual);
  }

  @Test
  public void testQ1() {
    String q1 = resourceToString("tpch/queries/q1.ppl");
    String actual = execute(q1);
    assertEquals("", actual);
  }

  @Test
  public void testQ2() {
    String q2 = resourceToString("tpch/queries/q2.ppl");
    String actual = execute(q2);
    assertEquals("", actual);
  }

  @Test
  public void testQ3() {
    String q3 = resourceToString("tpch/queries/q3.ppl");
    String actual = execute(q3);
    assertEquals("", actual);
  }

  @Test
  public void testQ4() {
    String q4 = resourceToString("tpch/queries/q4.ppl");
    String actual = execute(q4);
    assertEquals("", actual);
  }

  @Test
  public void testQ5() {
    String q5 = resourceToString("tpch/queries/q5.ppl");
    String actual = execute(q5);
    assertEquals("", actual);
  }

  @Test
  public void testQ6() {
    String q6 = resourceToString("tpch/queries/q6.ppl");
    String actual = execute(q6);
    assertEquals("", actual);
  }

  @Test
  public void testQ7() {
    String q7 = resourceToString("tpch/queries/q7.ppl");
    String actual = execute(q7);
    assertEquals("", actual);
  }

  @Test
  public void testQ8() {
    String q8 = resourceToString("tpch/queries/q8.ppl");
    String actual = execute(q8);
    assertEquals("", actual);
  }

  @Test
  public void testQ9() {
    String q9 = resourceToString("tpch/queries/q9.ppl");
    String actual = execute(q9);
    assertEquals("", actual);
  }

  @Test
  public void testQ10() {
    String q10 = resourceToString("tpch/queries/q10.ppl");
    String actual = execute(q10);
    assertEquals("", actual);
  }

  @Test
  public void testQ11() {
    String q11 = resourceToString("tpch/queries/q11.ppl");
    String actual = execute(q11);
    assertEquals("", actual);
  }

  @Test
  public void testQ12() {
    String q12 = resourceToString("tpch/queries/q12.ppl");
    String actual = execute(q12);
    assertEquals("", actual);
  }

  @Test
  public void testQ13() {
    String q13 = resourceToString("tpch/queries/q13.ppl");
    String actual = execute(q13);
    assertEquals("", actual);
  }

  @Test
  public void testQ14() {
    String q14 = resourceToString("tpch/queries/q14.ppl");
    String actual = execute(q14);
    assertEquals("", actual);
  }

  @Test
  public void testQ15() {
    String q15 = resourceToString("tpch/queries/q15.ppl");
    String actual = execute(q15);
    assertEquals("", actual);
  }

  @Test
  public void testQ16() {
    String q16 = resourceToString("tpch/queries/q16.ppl");
    String actual = execute(q16);
    assertEquals("", actual);
  }

  @Test
  public void testQ17() {
    String q17 = resourceToString("tpch/queries/q17.ppl");
    String actual = execute(q17);
    assertEquals("", actual);
  }

  @Test
  public void testQ18() {
    String q18 = resourceToString("tpch/queries/q18.ppl");
    String actual = execute(q18);
    assertEquals("", actual);
  }

  @Test
  public void testQ19() {
    String q19 = resourceToString("tpch/queries/q19.ppl");
    String actual = execute(q19);
    assertEquals("", actual);
  }

  @Test
  public void testQ20() {
    String q20 = resourceToString("tpch/queries/q20.ppl");
    String actual = execute(q20);
    assertEquals("", actual);
  }

  @Test
  public void testQ21() {
    String q21 = resourceToString("tpch/queries/q21.ppl");
    String actual = execute(q21);
    assertEquals("", actual);
  }

  @Test
  public void testQ22() {
    String q22 = resourceToString("tpch/queries/q22.ppl");
    String actual = execute(q22);
    assertEquals("", actual);
  }
}
