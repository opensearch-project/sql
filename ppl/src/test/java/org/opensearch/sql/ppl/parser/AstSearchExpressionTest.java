/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

public class AstSearchExpressionTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();
  private final Settings settings = mock(Settings.class);

  @Test
  public void testSimpleSearchTerm() {
    String query = "search \"error\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Double quotes are preserved for phrase search
    assertEquals("error", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testFieldComparison() {
    String query = "search status=200 source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("status:200", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testAndExpression() {
    String query = "search status=200 AND message=\"success\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("(status:200 AND message:success)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testOrExpression() {
    String query = "search error OR warning source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("(error OR warning)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testNotExpression() {
    String query = "search NOT error source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("NOT(error)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testGroupedExpression() {
    String query = "search (error OR warning) AND status=500 source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("(((error OR warning)) AND status:500)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testInList() {
    String query = "search status IN (200, 201, 204) source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("status:( 200 OR 201 OR 204 )", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testComparisonOperators() {
    String query = "search age>18 source=users";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("age:>18", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("users", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testNotEqualsOperator() {
    String query = "search status!=200 source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // != means field must exist and not equal to value
    assertEquals("( _exists_:status AND NOT status:200 )", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testNotEqualsWithAndExpression() {
    String query = "search status!=500 AND level=\"INFO\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals(
        "(( _exists_:status AND NOT status:500 ) AND level:INFO)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testNotVsNotEquals() {
    // Test the difference between NOT field:value and field!=value

    // NOT field:value - returns everything except field="value", including docs where field doesn't
    // exist
    String query1 = "search NOT status=\"200\" source=logs";
    Node plan1 = buildPlan(query1);
    assertTrue(plan1 instanceof Search);
    Search search1 = (Search) plan1;
    assertEquals("NOT(status:200)", search1.getQueryString());

    // field!=value - returns only docs where field exists AND is not "value"
    String query2 = "search status!=\"200\" source=logs";
    Node plan2 = buildPlan(query2);
    assertTrue(plan2 instanceof Search);
    Search search2 = (Search) plan2;
    assertEquals("( _exists_:status AND NOT status:200 )", search2.getQueryString());
  }

  @Test
  public void testEscapedSpecialCharacters() {
    // Test escaping special characters in values
    String query = "search message=\"Error: (1+1)\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Special characters should be escaped
    assertEquals("message:\"Error\\: \\(1\\+1\\)\"", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testEscapedWildcardAndQuestion() {
    // Test that wildcards are NOT escaped to support pattern matching
    String query = "search filename=\"test*.txt?\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // * and ? should NOT be escaped to allow wildcard pattern matching
    assertEquals("filename:test*.txt?", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testEscapedBooleanOperators() {
    // Test escaping && and || in values
    String query = "search code=\"a && b || c\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // && and || should be escaped
    assertEquals("code:\"a \\&\\& b \\|\\| c\"", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testHyphenInStrings() {
    // Test that hyphens in string values are escaped
    String query = "search date=\"2024-01-01\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Hyphens in string values should be escaped
    assertEquals("date:2024\\-01\\-01", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testNegativeNumbers() {
    String query = "search a=1 b=-1 source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Negative numbers should be properly handled
    assertEquals("(a:1) AND (b:-1)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testMultipleSearchExpressionsCombinedWithAnd() {
    // Test that multiple searchExpressions without explicit AND/OR are combined with AND
    String query = "search status=200 message=\"success\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Multiple expressions should be combined with AND
    assertEquals("(status:200) AND (message:success)", search.getQueryString());

    // Check the child is a Relation
    assertTrue(search.getChild().get(0) instanceof Relation);
    Relation relation = (Relation) search.getChild().get(0);
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testMinimalEscapingForURL() {
    // URLs should work without escaping colons and slashes
    String query = "search url=\"http://api.example.com:8080/path\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Lucene special chars are escaped internally, user doesn't need to escape
    assertEquals("url:http\\:\\/\\/api.example.com\\:8080\\/path", search.getQueryString());
  }

  @Test
  public void testMinimalEscapingForEmail() {
    // Email addresses with + and @ should work without escaping
    String query = "search email=\"user+tag@company.com\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("email:user\\+tag@company.com", search.getQueryString());
  }

  @Test
  @Ignore("String Escaping is not handled properly in 2.19")
  public void testOnlyEscapeQuotesBackslashPipe() {
    // Test that only ", \, and | need escaping by user
    // Quotes must be escaped
    String query1 = "search message=\"She said \\\"Hello\\\"\" source=logs";
    Node plan1 = buildPlan(query1);
    Search search1 = (Search) plan1;
    assertEquals("message:\"She said \\\"Hello\\\"\"", search1.getQueryString());

    // Backslash must be escaped
    String query2 = "search path=\"C:\\\\\\\\Users\\\\\\\\file.txt\" source=logs";
    Node plan2 = buildPlan(query2);
    Search search2 = (Search) plan2;
    assertEquals("path:C\\:\\\\Users\\\\file.txt", search2.getQueryString());

    String query3 = "search code=\"a || b\" source=logs";
    Node plan3 = buildPlan(query3);
    Search search3 = (Search) plan3;
    assertEquals("code:\"a \\|\\| b\"", search3.getQueryString());
  }

  @Test
  public void testSpecialCharsNoEscapingNeeded() {
    // Test that common special characters work without user escaping
    String query = "search log=\"[ERROR] Failed: (reason #1)\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // These chars are escaped for Lucene internally, not by user
    assertEquals("log:\"\\[ERROR\\] Failed\\: \\(reason #1\\)\"", search.getQueryString());
  }

  @Test
  public void testWildcardsPreserved() {
    // Wildcards * and ? should not be escaped to allow pattern matching
    String query = "search file=\"*.log\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("file:*.log", search.getQueryString());

    String query2 = "search name=\"user?\" source=logs";
    Node plan2 = buildPlan(query2);
    Search search2 = (Search) plan2;
    assertEquals("name:user?", search2.getQueryString());
  }

  @Test
  public void testBasicIndexSearch() {
    String query = "search source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Relation);
    Relation relation = (Relation) plan;
    assertEquals("logs", relation.getTableQualifiedName().toString());
  }

  @Test
  public void testSingleAmpersandAtEndOfString() {
    // Test single & at the end of string - covers branch where i+1 >= text.length()
    String query = "search message=\"test&\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Single & at end should be escaped
    assertEquals("message:test\\&", search.getQueryString());
  }

  @Test
  public void testSinglePipeAtEndOfString() {
    // Test single | at the end of string - covers branch where i+1 >= text.length()
    String query = "search message=\"test|\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Single | at end should be escaped
    assertEquals("message:test\\|", search.getQueryString());
  }

  @Test
  public void testSingleAmpersandFollowedByDifferentChar() {
    // Test single & followed by a different character (not another &)
    String query = "search message=\"a&b\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Single & should be escaped
    assertEquals("message:a\\&b", search.getQueryString());
  }

  @Test
  public void testSinglePipeFollowedByDifferentChar() {
    // Test single | followed by a different character (not another |)
    String query = "search message=\"a|b\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Single | should be escaped
    assertEquals("message:a\\|b", search.getQueryString());
  }

  @Test
  public void testMixedSingleAndDoubleAmpersands() {
    // Test mix of single & and double &&
    String query = "search message=\"a & b && c\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Single & should be escaped, double && should be double-escaped
    assertEquals("message:\"a \\& b \\&\\& c\"", search.getQueryString());
  }

  @Test
  public void testMixedSingleAndDoublePipes() {
    // Test mix of single | and double ||
    String query = "search message=\"a | b || c\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Single | should be escaped, double || should be double-escaped
    assertEquals("message:\"a \\| b \\|\\| c\"", search.getQueryString());
  }

  @Test
  @Ignore("String Escaping is not handled properly in 2.19")
  public void testAllLuceneSpecialCharacters() {
    // Test all special characters in LUCENE_SPECIAL_CHARS
    String query = "search message=\"+test-&|!(){}[]^\\\"~:/\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // All special chars should be escaped except wildcards
    assertEquals(
        "message:\\+test\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\:\\/", search.getQueryString());
  }

  @Test
  public void testNestedFieldAccess() {
    // Test nested field access with dots
    String query = "search user.name=\"john\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Dots in field names should not be escaped
    assertEquals("user.name:john", search.getQueryString());
  }

  @Test
  public void testComplexNestedField() {
    // Test complex nested field path
    String query = "search request.headers.user_agent=\"Mozilla\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // Dots in field names should not be escaped
    assertEquals("request.headers.user_agent:Mozilla", search.getQueryString());
  }

  @Test
  public void testEmptyStringValue() {
    // Test empty string value
    String query = "search myfield=\"\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    assertEquals("myfield:", search.getQueryString());
  }

  @Test
  public void testSingleCharacterSpecialValues() {
    // Test single special characters as values
    String query1 = "search myfield=\"&\" source=logs";
    Node plan1 = buildPlan(query1);
    Search search1 = (Search) plan1;
    assertEquals("myfield:\\&", search1.getQueryString());

    String query2 = "search myfield=\"|\" source=logs";
    Node plan2 = buildPlan(query2);
    Search search2 = (Search) plan2;
    assertEquals("myfield:\\|", search2.getQueryString());
  }

  private Node buildPlan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }
}
