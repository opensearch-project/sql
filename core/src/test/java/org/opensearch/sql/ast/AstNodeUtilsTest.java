/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

@ExtendWith(MockitoExtension.class)
public class AstNodeUtilsTest {

  @Mock private UnresolvedPlan mockPlan;
  @Mock private UnresolvedExpression mockExpr;
  @Mock private Node mockNode;
  @Mock private Node mockParent;
  @Mock private Node mockChild;
  @Mock private Node mockGrandchild;

  private ScalarSubquery scalarSubquery;
  private ExistsSubquery existsSubquery;
  private InSubquery inSubquery;

  @BeforeEach
  public void setUp() {
    scalarSubquery = new ScalarSubquery(mockPlan);
    existsSubquery = new ExistsSubquery(mockPlan);
    inSubquery = new InSubquery(Collections.singletonList(mockExpr), mockPlan);
  }

  @Test
  public void testContainsSubqueryExpressionWithNull() {
    assertFalse(AstNodeUtils.containsSubqueryExpression(null));
  }

  @Test
  public void testContainsSubqueryExpressionWithScalarSubquery() {
    assertTrue(AstNodeUtils.containsSubqueryExpression(scalarSubquery));
  }

  @Test
  public void testContainsSubqueryExpressionWithExistsSubquery() {
    assertTrue(AstNodeUtils.containsSubqueryExpression(existsSubquery));
  }

  @Test
  public void testContainsSubqueryExpressionWithInSubquery() {
    assertTrue(AstNodeUtils.containsSubqueryExpression(inSubquery));
  }

  @Test
  public void testContainsSubqueryExpressionWithLetContainingSubquery() {
    Field field = new Field(QualifiedName.of("test"));
    Let letExpr = new Let(field, scalarSubquery);
    assertTrue(AstNodeUtils.containsSubqueryExpression(letExpr));
  }

  @Test
  public void testContainsSubqueryExpressionWithLetNotContainingSubquery() {
    Literal literal = new Literal(42, null);
    Field field = new Field(QualifiedName.of("test"));
    Let letExpr = new Let(field, literal);
    assertFalse(AstNodeUtils.containsSubqueryExpression(letExpr));
  }

  @Test
  public void testContainsSubqueryExpressionWithSimpleExpression() {
    Literal literal = new Literal(42, null);
    assertFalse(AstNodeUtils.containsSubqueryExpression(literal));
  }

  @Test
  public void testContainsSubqueryExpressionWithNodeHavingSubqueryChild() {
    doReturn(Collections.singletonList(scalarSubquery)).when(mockParent).getChild();
    assertTrue(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithNodeHavingNoSubqueryChild() {
    Literal literal = new Literal(42, null);
    doReturn(Collections.singletonList(literal)).when(mockParent).getChild();
    assertFalse(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithNodeHavingMultipleChildren() {
    Literal literal1 = new Literal(1, null);
    Literal literal2 = new Literal(2, null);
    doReturn(Arrays.asList(literal1, scalarSubquery, literal2)).when(mockParent).getChild();
    assertTrue(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithNodeHavingNoChildren() {
    doReturn(Collections.emptyList()).when(mockNode).getChild();
    assertFalse(AstNodeUtils.containsSubqueryExpression(mockNode));
  }

  @Test
  public void testContainsSubqueryExpressionWithNestedStructure() {
    doReturn(Collections.singletonList(scalarSubquery)).when(mockChild).getChild();
    doReturn(Collections.singletonList(mockChild)).when(mockParent).getChild();
    assertTrue(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithDeeplyNestedStructure() {
    doReturn(Collections.singletonList(scalarSubquery)).when(mockGrandchild).getChild();
    doReturn(Collections.singletonList(mockGrandchild)).when(mockChild).getChild();
    doReturn(Collections.singletonList(mockChild)).when(mockParent).getChild();
    assertTrue(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithLetNestedInNode() {
    Field field = new Field(QualifiedName.of("test"));
    Let letExpr = new Let(field, scalarSubquery);
    doReturn(Collections.singletonList(letExpr)).when(mockParent).getChild();
    assertTrue(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithMultipleLetExpressions() {
    Field field1 = new Field(QualifiedName.of("test1"));
    Let letWithSubquery = new Let(field1, scalarSubquery);

    Literal literal = new Literal(42, null);
    Field field2 = new Field(QualifiedName.of("test2"));
    Let letWithoutSubquery = new Let(field2, literal);

    doReturn(Arrays.asList(letWithoutSubquery, letWithSubquery)).when(mockParent).getChild();
    assertTrue(AstNodeUtils.containsSubqueryExpression(mockParent));
  }

  @Test
  public void testContainsSubqueryExpressionWithComplexNestedLet() {
    Field innerField = new Field(QualifiedName.of("inner"));
    Let innerLet = new Let(innerField, scalarSubquery);

    Field outerField = new Field(QualifiedName.of("outer"));
    Let outerLet = new Let(outerField, innerLet);

    assertTrue(AstNodeUtils.containsSubqueryExpression(outerLet));
  }
}
