/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.junit.Test;

public class PositionFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.CALCS);
  }

  @Test
  public void test_position_function() throws IOException {
    String query = "source=" + TEST_INDEX_CALCS + " | eval f=position('ON', str1) | fields f";

    var result = executeQuery(query);

    assertEquals(17, result.getInt("total"));
    verifyDataRows(
        result, rows(7), rows(7), rows(2), rows(0), rows(0), rows(0), rows(0), rows(0), rows(0),
        rows(0), rows(0), rows(0), rows(0), rows(0), rows(0), rows(0), rows(0));
  }

  @Test
  public void test_position_function_with_fields_only() throws IOException {
    String query =
        "source="
            + TEST_INDEX_CALCS
            + " | eval f=position(str3 IN str2) | where str2 IN ('one', 'two', 'three')| fields f";

    var result = executeQuery(query);

    assertEquals(3, result.getInt("total"));
    verifyDataRows(result, rows(3), rows(0), rows(4));
  }

  @Test
  public void test_position_function_with_string_literals() throws IOException {
    String query =
        "source="
            + TEST_INDEX_CALCS
            + " | eval f=position('world' IN 'hello world') | where str2='one' | fields f";

    var result = executeQuery(query);

    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows(7));
  }

  @Test
  public void test_position_function_with_nulls() throws IOException {
    String query =
        "source="
            + TEST_INDEX_CALCS
            + " | eval f=position('ee' IN str2) | where isnull(str2) | fields str2,f";

    var result = executeQuery(query);

    assertEquals(4, result.getInt("total"));
    verifyDataRows(result, rows(null, null), rows(null, null), rows(null, null), rows(null, null));
  }

  @Test
  public void test_position_function_with_function_as_arg() throws IOException {
    String query =
        "source="
            + TEST_INDEX_CALCS
            + " | eval f=position(upper(str3) IN str1) | where like(str1, 'BINDING SUPPLIES') |"
            + " fields f";

    var result = executeQuery(query);

    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows(15));
  }

  @Test
  public void test_position_function_with_function_in_where_clause() throws IOException {
    String query = "source=" + TEST_INDEX_CALCS + " | where position(str3 IN str2)=1 | fields str2";

    var result = executeQuery(query);

    assertEquals(2, result.getInt("total"));
    verifyDataRows(result, rows("eight"), rows("eleven"));
  }
}
