/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrderByClauseContext;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.NullOrder;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.Sort.SortOrder;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

/**
 * AST sort builder that builds Sort AST node from ORDER BY clause. During this process, the item
 * in order by may be replaced by item in project list if it's an alias or ordinal. This is same as
 * GROUP BY building process.
 */
@RequiredArgsConstructor
public class AstSortBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedPlan> {

  private final QuerySpecification querySpec;

  @Override
  public UnresolvedPlan visitOrderByClause(OrderByClauseContext ctx) {
    return new Sort(
        createSortFields()
    );
  }

  private List<Field> createSortFields() {
    List<Field> fields = new ArrayList<>();
    List<UnresolvedExpression> items = querySpec.getOrderByItems();
    List<SortOption> options = querySpec.getOrderByOptions();
    for (int i = 0; i < items.size(); i++) {
      fields.add(
          new Field(
              querySpec.replaceIfAliasOrOrdinal(items.get(i)),
              createSortArguments(options.get(i))));
    }
    return fields;
  }

  /**
   * Argument "asc" is required.
   * Argument "nullFirst" is optional and determined by Analyzer later if absent.
   */
  private List<Argument> createSortArguments(SortOption option) {
    SortOrder sortOrder = option.getSortOrder();
    NullOrder nullOrder = option.getNullOrder();
    ImmutableList.Builder<Argument> args = ImmutableList.builder();
    args.add(new Argument("asc", booleanLiteral(sortOrder != DESC))); // handle both null and ASC

    if (nullOrder != null) {
      args.add(new Argument("nullFirst", booleanLiteral(nullOrder == NULL_FIRST)));
    }
    return args.build();
  }

}
