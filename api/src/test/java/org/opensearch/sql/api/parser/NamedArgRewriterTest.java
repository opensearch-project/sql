/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

/** Unit tests for {@link NamedArgRewriter}. */
public class NamedArgRewriterTest {

  /** Match production parser config in UnifiedQueryContext. */
  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.Config.DEFAULT.withUnquotedCasing(Casing.UNCHANGED);

  @Test
  public void testPositionalArgsRewrittenToMaps() throws Exception {
    SqlNode result = rewrite("SELECT * FROM t WHERE \"match\"(name, 'John')");
    assertContains(result, "MAP['field', `name`], MAP['query', 'John']");
  }

  @Test
  public void testEqualsArgRewrittenToMap() throws Exception {
    SqlNode result = rewrite("SELECT * FROM t WHERE \"match\"(name, 'John', operator='AND')");
    assertContains(result, "MAP['query', 'John'], MAP['operator', 'AND']");
  }

  @Test
  public void testMultipleEqualsArgs() throws Exception {
    SqlNode result =
        rewrite("SELECT * FROM t WHERE \"match\"(name, 'John', operator='AND', boost=2.0)");
    assertContains(result, "MAP['operator', 'AND'], MAP['boost', 2.0]");
  }

  @Test
  public void testMultiMatchUsesFieldsParamName() throws Exception {
    SqlNode result = rewrite("SELECT * FROM t WHERE multi_match(name, 'John')");
    assertContains(result, "MAP['fields', `name`], MAP['query', 'John']");
  }

  @Test
  public void testNonRelevanceFunctionUntouched() throws Exception {
    SqlNode parsed = parse("SELECT upper(name) FROM t");
    SqlNode result = parsed.accept(NamedArgRewriter.INSTANCE);
    assertSame(parsed, result);
  }

  @Test
  public void testAllEqualsArgsNoPositional() throws Exception {
    // Not valid V2 match syntax, but multi_match supports this form.
    // Shuttle treats all = as named options — no positional wrapping.
    SqlNode result = rewrite("SELECT * FROM t WHERE multi_match(fields=name, query='John')");
    assertContains(result, "MAP['fields', `name`], MAP['query', 'John']");
  }

  @Test
  public void testEqualsBeforePositionalThrows() throws Exception {
    // Not valid V2 syntax — positional must come first.
    // = at index 0 goes to EQUALS branch, but remaining positional args exceed paramNames.
    try {
      rewrite("SELECT * FROM t WHERE \"match\"(operator='AND', name, 'John')");
      fail("Expected IndexOutOfBoundsException for mixed order");
    } catch (IndexOutOfBoundsException e) {
      // Expected: = consumes index 0, positional 'name' maps to paramNames[1],
      // positional 'John' tries paramNames[2] which doesn't exist
    }
  }

  @Test
  public void testExtraPositionalArgsBeyondParamNamesThrows() throws Exception {
    // match has 2 param names (field, query); 3 positional args causes IndexOutOfBounds
    try {
      rewrite("SELECT * FROM t WHERE \"match\"(a, b, c)");
      fail("Expected IndexOutOfBoundsException for extra positional args");
    } catch (IndexOutOfBoundsException e) {
      // Expected: paramNames has 2 entries but 3 positional args
    }
  }

  private static SqlNode rewrite(String sql) throws Exception {
    return parse(sql).accept(NamedArgRewriter.INSTANCE);
  }

  private static SqlNode parse(String sql) throws Exception {
    return SqlParser.create(sql, PARSER_CONFIG).parseStmt();
  }

  private static void assertContains(SqlNode node, String expected) {
    String actual = node.toString().replaceAll("\\n", " ");
    assertTrue(
        "Expected to contain: " + expected + "\nActual: " + actual, actual.contains(expected));
  }
}
