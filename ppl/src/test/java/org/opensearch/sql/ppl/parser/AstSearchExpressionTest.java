/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
    assertEquals("\"error\"", search.getQueryString());

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
    // Test escaping wildcard and question mark
    String query = "search filename=\"test*.txt?\" source=logs";
    Node plan = buildPlan(query);

    assertTrue(plan instanceof Search);
    Search search = (Search) plan;
    // * and ? should be escaped (no quotes because no spaces in value)
    assertEquals("filename:test\\*.txt\\?", search.getQueryString());

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

  private Node buildPlan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }
}
