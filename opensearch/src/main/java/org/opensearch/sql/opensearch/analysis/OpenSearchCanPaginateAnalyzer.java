/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.analysis;

import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.executor.pagination.CanPaginateVisitor;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.ast.expression.HighlightFunction;
import org.opensearch.sql.opensearch.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.opensearch.ast.expression.RelevanceFieldList;
import org.opensearch.sql.opensearch.ast.expression.ScoreFunction;

public class OpenSearchCanPaginateAnalyzer extends CanPaginateVisitor implements OpenSearchAbstractNodeVisitor<Boolean, Object> {

  @Override
  public Boolean visitRelevanceFieldList(RelevanceFieldList node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitHighlightFunction(HighlightFunction node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitScoreFunction(ScoreFunction node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitNestedAllTupleFields(NestedAllTupleFields node, Object context) {
    return Boolean.FALSE;
  }

  // Only column references in ORDER BY clause are supported in pagination,
  // because expressions can't be pushed down due to #1471.
  // https://github.com/opensearch-project/sql/issues/1471
  @Override
  public Boolean visitSort(Sort node, Object context) {
    return node.getSortList().stream()
            .allMatch(f -> f.getField() instanceof QualifiedName && visitField(f, context))
        && canPaginate(node, context);
  }

  @Override
  public Boolean visitFunction(Function node, Object context) {
    // https://github.com/opensearch-project/sql/issues/1718
    if (node.getFuncName()
        .equalsIgnoreCase(BuiltinFunctionName.NESTED.getName().getFunctionName())) {
      return Boolean.FALSE;
    }
    return canPaginate(node, context);
  }

  // Queries with LIMIT/OFFSET clauses are unsupported
  @Override
  public Boolean visitLimit(Limit node, Object context) {
    return Boolean.FALSE;
  }

  // Queries with GROUP BY clause are not supported
  @Override
  public Boolean visitAggregation(Aggregation node, Object context) {
    return Boolean.FALSE;
  }
}
