/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.having;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.Limit;

public class ParserUtilsTest {
  @Test
  public void testFindNodes() {
    Node root =
        project(
            filter(
                having(
                    agg(
                        relation("test"),
                        ImmutableList.of(
                            alias("AVG(age)", aggregate("AVG", qualifiedName("age"))),
                            alias("MIN(balance)", aggregate("MIN", qualifiedName("balance")))),
                        emptyList(),
                        ImmutableList.of(alias("name", qualifiedName("name"))),
                        emptyList(),
                        ImmutableList.of(
                            qualifiedName("name"), aggregate("AVG", qualifiedName("age")))),
                    ImmutableList.of(
                        alias("MIN(balance)", aggregate("MIN", qualifiedName("balance")))),
                    function(">", aggregate("MIN", qualifiedName("balance")), intLiteral(1000))),
                function(">", aggregate("MIN", qualifiedName("balance")), intLiteral(1000))),
            alias("name", qualifiedName("name")),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age"))));
    // test finding a UnresolvedPlan
    Node aggNode = ParserUtils.findNodes(root, n -> n instanceof Aggregation).getFirst();
    assertInstanceOf(Aggregation.class, aggNode);
    // test finding a UnresolvedExpression
    UnresolvedExpression expr = ((Aggregation) aggNode).getAggExprList().getFirst();
    Node aliasNode = ParserUtils.findNodes(expr, n -> n instanceof Alias).getFirst();
    assertInstanceOf(Alias.class, aliasNode);
    assertEquals("AVG(age)", ((Alias) aliasNode).getName());
    // test finding a nonexistent node
    List<Node> nodes = ParserUtils.findNodes(root, n -> n instanceof Limit);
    assertTrue(nodes.isEmpty());
  }

  @Test
  public void testFindNodesOnNodeWithoutChild() {
    Node root = project(new FetchCursor("test"), alias("name", qualifiedName("name")));
    Node child = ParserUtils.findNodes(root, n -> n instanceof FetchCursor).getFirst();
    assertInstanceOf(FetchCursor.class, child);
    // FetchCursor.getChild() return null
    assertTrue(ParserUtils.findNodes(child, n -> n instanceof Limit).isEmpty());
  }
}
