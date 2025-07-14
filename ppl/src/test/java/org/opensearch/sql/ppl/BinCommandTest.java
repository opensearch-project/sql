/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.decimalLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class BinCommandTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  @Test
  public void testBasicBinCommand() {
    assertEqual(
        "source=t | bin age span=10",
        new Bin(field("age"), intLiteral(10), null, null, null, null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithAlias() {
    assertEqual(
        "source=t | bin age span=10 AS age_group",
        new Bin(field("age"), intLiteral(10), null, null, null, "age_group").attach(relation("t")));
  }

  @Test
  public void testBinCommandWithBinsParameter() {
    assertEqual(
        "source=t | bin score bins=5",
        new Bin(field("score"), null, 5, null, null, null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithStartAndEnd() {
    assertEqual(
        "source=t | bin value span=5 start=0 end=100",
        new Bin(field("value"), intLiteral(5), null, intLiteral(0), intLiteral(100), null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithAllParameters() {
    assertEqual(
        "source=t | bin price bins=10 start=0 end=1000 AS price_range",
        new Bin(field("price"), null, 10, intLiteral(0), intLiteral(1000), "price_range").attach(relation("t")));
  }

  @Test
  public void testBinCommandWithSpanAndBins() {
    assertEqual(
        "source=t | bin temperature span=5 bins=20",
        new Bin(field("temperature"), intLiteral(5), 20, null, null, null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithStringSpan() {
    assertEqual(
        "source=t | bin timestamp span=\"1h\"",
        new Bin(field("timestamp"), stringLiteral("1h"), null, null, null, null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithDecimalValues() {
    assertEqual(
        "source=t | bin amount span=2.5 start=0.0 end=100.0",
        new Bin(field("amount"), decimalLiteral(2.5), null, decimalLiteral(0.0), decimalLiteral(100.0), null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithBacktickedField() {
    assertEqual(
        "source=t | bin `field.name` span=10",
        new Bin(field("field.name"), intLiteral(10), null, null, null, null).attach(relation("t")));
  }

  @Test
  public void testBinCommandWithBacktickedAlias() {
    assertEqual(
        "source=t | bin age span=10 AS `age group`",
        new Bin(field("age"), intLiteral(10), null, null, null, "age group").attach(relation("t")));
  }

  @Test(expected = SyntaxCheckException.class)
  public void testBinCommandWithoutFieldThrowsException() {
    plan("source=t | bin span=10");
  }

  @Test
  public void testBinCommandWithoutParametersIsValid() {
    // A bin command with just a field is actually valid - it uses default binning
    assertEqual(
        "source=t | bin age",
        new Bin(field("age"), null, null, null, null, null).attach(relation("t")));
  }

  @Test(expected = SyntaxCheckException.class)
  public void testBinCommandWithInvalidSyntaxThrowsException() {
    plan("source=t | bin age span");
  }

  @Test
  public void testBinCommandASTNodeCreation() {
    Node actualPlan = plan("source=t | bin age span=10 AS age_group");
    
    // Verify that the AST structure is correct
    assertEquals(true, actualPlan instanceof Bin);
    
    Bin binNode = (Bin) actualPlan;
    assertEquals(field("age"), binNode.getField());
    assertEquals(intLiteral(10), binNode.getSpan());
    assertEquals(null, binNode.getBins());
    assertEquals(null, binNode.getStart());
    assertEquals(null, binNode.getEnd());
    assertEquals("age_group", binNode.getAlias());
    
    // Verify that it has a child (the relation)
    assertEquals(1, binNode.getChild().size());
  }

  @Test
  public void testBinCommandWithComplexFieldName() {
    // Test with backticks for field names with dots
    assertEqual(
        "source=logs | bin `nested.field.value` span=100",
        new Bin(field("nested.field.value"), intLiteral(100), null, null, null, null).attach(relation("logs")));
  }

  @Test
  public void testBinCommandParameterParsing() {
    Node actualPlan = plan("source=t | bin score bins=5 start=0 end=100");
    
    Bin binNode = (Bin) actualPlan;
    assertEquals(field("score"), binNode.getField());
    assertEquals(null, binNode.getSpan());
    assertEquals(Integer.valueOf(5), binNode.getBins());
    assertEquals(intLiteral(0), binNode.getStart());
    assertEquals(intLiteral(100), binNode.getEnd());
    assertEquals(null, binNode.getAlias());
  }

  protected void assertEqual(String query, Node expectedPlan) {
    Node actualPlan = plan(query);
    assertEquals(expectedPlan, actualPlan);
  }

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(query);
    return astBuilder.visit(parser.parse(query));
  }
}