/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tpch;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.calcite.standalone.CalcitePPLIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

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

  String loadFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      return new String(Files.readAllBytes(Paths.get(uri)));
    } catch (Exception e) {
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
    String ppl = loadFromFile("tpch/queries/q1.ppl");
    String plan = explain(ppl);
    assertEquals("", plan);
  }

  @Test
  public void testQ2() {
    String q2 = loadFromFile("tpch/queries/q2.ppl");
    String actual = execute(q2);
    assertEquals("", actual);
  }

  @Test
  public void testQ3() {
    String q3 = loadFromFile("tpch/queries/q3.ppl");
    String actual = execute(q3);
    assertEquals("", actual);
  }

  @Test
  public void testQ4() {
    String q4 = loadFromFile("tpch/queries/q4.ppl");
    String actual = execute(q4);
    assertEquals("", actual);
  }

  @Test
  public void testQ5() {
    String q5 = loadFromFile("tpch/queries/q5.ppl");
    String actual = execute(q5);
    assertEquals("", actual);
  }

  @Test
  public void testQ6() {
    String q6 = loadFromFile("tpch/queries/q6.ppl");
    String actual = execute(q6);
    assertEquals("", actual);
  }

  @Test
  public void testQ7() {
    String q7 = loadFromFile("tpch/queries/q7.ppl");
    String actual = execute(q7);
    assertEquals("", actual);
  }

  @Test
  public void testQ8() {
    String q8 = loadFromFile("tpch/queries/q8.ppl");
    String actual = execute(q8);
    assertEquals("", actual);
  }

  @Test
  public void testQ9() {
    String q9 = loadFromFile("tpch/queries/q9.ppl");
    String actual = execute(q9);
    assertEquals("", actual);
  }

  @Test
  public void testQ10() {
    String q10 = loadFromFile("tpch/queries/q10.ppl");
    String actual = execute(q10);
    assertEquals("", actual);
  }

  @Test
  public void testQ11() {
    String q11 = loadFromFile("tpch/queries/q11.ppl");
    String actual = execute(q11);
    assertEquals("", actual);
  }

  @Test
  public void testQ12() {
    String q12 = loadFromFile("tpch/queries/q12.ppl");
    String actual = execute(q12);
    assertEquals("", actual);
  }

  @Test
  public void testQ13() {
    String q13 = loadFromFile("tpch/queries/q13.ppl");
    String actual = execute(q13);
    assertEquals("", actual);
  }

  @Test
  public void testQ14() {
    String q14 = loadFromFile("tpch/queries/q14.ppl");
    String actual = execute(q14);
    assertEquals("", actual);
  }

  @Test
  public void testQ15() {
    String q15 = loadFromFile("tpch/queries/q15.ppl");
    String actual = execute(q15);
    assertEquals("", actual);
  }

  @Test
  public void testQ16() {
    String q16 = loadFromFile("tpch/queries/q16.ppl");
    String actual = execute(q16);
    assertEquals("", actual);
  }

  @Test
  public void testQ17() {
    String q17 = loadFromFile("tpch/queries/q17.ppl");
    String actual = execute(q17);
    assertEquals("", actual);
  }

  @Test
  public void testQ18() {
    String q18 = loadFromFile("tpch/queries/q18.ppl");
    String actual = execute(q18);
    assertEquals("", actual);
  }

  @Test
  public void testQ19() {
    String q19 = loadFromFile("tpch/queries/q19.ppl");
    String actual = execute(q19);
    assertEquals("", actual);
  }

  @Test
  public void testQ20() {
    String q20 = loadFromFile("tpch/queries/q20.ppl");
    String actual = execute(q20);
    assertEquals("", actual);
  }

  @Test
  public void testQ21() {
    String q21 = loadFromFile("tpch/queries/q21.ppl");
    String actual = execute(q21);
    assertEquals("", actual);
  }

  @Test
  public void testQ22() {
    String q22 = loadFromFile("tpch/queries/q22.ppl");
    String actual = execute(q22);
    assertEquals("", actual);
  }
}
