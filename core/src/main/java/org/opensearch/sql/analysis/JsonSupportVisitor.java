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
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;

/**
 * This visitor's sole purpose is to throw UnsupportedOperationExceptions
 * for unsupported features in the new engine when JSON format is specified.
 * Unsupported features in V2 are ones the produce results that differ from
 * legacy results.
 */
public class JsonSupportVisitor extends AbstractNodeVisitor<Boolean, JsonSupportVisitorContext> {
  @Override
  public Boolean visit(Node node, JsonSupportVisitorContext context) {
    // UnresolvedPlan can only have one child until joins are supported
    return node.getChild().get(0).accept(this, context);
  }

  @Override
  protected Boolean defaultResult() {
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitLimit(Limit node, JsonSupportVisitorContext context) {
    context.addToUnsupportedNodes("limit");
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitAggregation(Aggregation node, JsonSupportVisitorContext context) {
    if (!node.getGroupExprList().isEmpty()) {
      context.addToUnsupportedNodes("aggregation");
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitFunction(Function node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // queries with function calls are not supported.
      context.addToUnsupportedNodes("functions");
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitLiteral(Literal node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // queries with literal values are not supported
      context.addToUnsupportedNodes("literal");
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitCast(Cast node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // Queries with cast are not supported
      context.addToUnsupportedNodes("cast");
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitAlias(Alias node, JsonSupportVisitorContext context) {
    // Supported if outside of Project
    if (context.isVisitingProject()) {
      // Alias node is accepted if it does not have a user-defined alias
      // and if the delegated expression is accepted.
      if (StringUtils.isEmpty(node.getAlias())) {
        return node.getDelegated().accept(this, context);
      } else {
        context.addToUnsupportedNodes("alias");
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitProject(Project node, JsonSupportVisitorContext context) {
    Boolean isSupported = visit(node, context);

    context.setVisitingProject(true);
    isSupported = node.getProjectList().stream()
        .allMatch(e -> e.accept(this, context)) && isSupported;
    context.setVisitingProject(false);
    return isSupported;
  }
}
