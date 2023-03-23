/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Project;

/**
 * This visitor's sole purpose is to throw UnsupportedOperationExceptions
 * for unsupported features in the new engine when JSON format is specified.
 * Unsupported features in V2 are ones the produce results that differ from
 * legacy results.
 */
public class JsonSupportVisitor extends AbstractNodeVisitor<Void, JsonSupportVisitorContext> {
  @Override
  public Void visit(Node node, JsonSupportVisitorContext context) {
    visitChildren(node, context);
    return null;
  }

  @Override
  public Void visitChildren(Node node, JsonSupportVisitorContext context) {
    for (Node child : node.getChild()) {
      child.accept(this, context);
    }
    return null;
  }

  @Override
  public Void visitAggregation(Aggregation node, JsonSupportVisitorContext context) {
    if (!node.getGroupExprList().isEmpty()) {
      throw new UnsupportedOperationException(
          "Queries with aggregation are not yet supported with json format in the new engine");
    }
    return null;
  }

  @Override
  public Void visitFunction(Function node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // queries with function calls are not supported.
      throw new UnsupportedOperationException(
          "Queries with functions are not yet supported with json format in the new engine");
    }
    return null;
  }

  @Override
  public Void visitLiteral(Literal node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // queries with literal values are not supported
      throw new UnsupportedOperationException(
          "Queries with literals are not yet supported with json format in the new engine");
    }
    return null;
  }

  @Override
  public Void visitCast(Cast node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // Queries with cast are not supported
      throw new UnsupportedOperationException(
          "Queries with casts are not yet supported with json format in the new engine");
    }
    return null;
  }

  @Override
  public Void visitAlias(Alias node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // Alias node is accepted if it does not have a user-defined alias
      // and if the delegated expression is accepted.
      if (StringUtils.isEmpty(node.getAlias())) {
        node.getDelegated().accept(this, context);
      } else {
        throw new UnsupportedOperationException(
            "Queries with aliases are not yet supported with json format in the new engine");
      }
    }
    return null;
  }

  @Override
  public Void visitProject(Project node, JsonSupportVisitorContext context) {
    visit(node, context);

    context.setVisitingProject(true);
    node.getProjectList().forEach(e -> e.accept(this, context));
    context.setVisitingProject(false);
    return null;
  }
}
