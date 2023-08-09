/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.conditional.cases;

import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * A CASE clause is very different from a regular function. Functions have well-defined signature,
 * though CASE clause is more like a function implementation which requires type check "manually".
 */
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class CaseClause extends FunctionExpression {

  /** List of WHEN clauses. */
  private final List<WhenClause> whenClauses;

  /** Default result if none of WHEN conditions match. */
  private final Expression defaultResult;

  /** Initialize case clause. */
  public CaseClause(List<WhenClause> whenClauses, Expression defaultResult) {
    super(FunctionName.of("case"), concatArgs(whenClauses, defaultResult));
    this.whenClauses = whenClauses;
    this.defaultResult = defaultResult;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    for (WhenClause when : whenClauses) {
      if (when.isTrue(valueEnv)) {
        return when.valueOf(valueEnv);
      }
    }
    return (defaultResult == null) ? ExprNullValue.of() : defaultResult.valueOf(valueEnv);
  }

  @Override
  public ExprType type() {
    List<ExprType> types = allResultTypes();

    // Return undefined if all WHEN/ELSE return NULL
    return types.isEmpty() ? UNDEFINED : types.get(0);
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitCase(this, context);
  }

  /**
   * Get types of each result in WHEN clause and ELSE clause. Exclude UNKNOWN type from NULL literal
   * which means NULL in THEN or ELSE clause is not included in result.
   *
   * @return all result types. Use list so caller can generate friendly error message.
   */
  public List<ExprType> allResultTypes() {
    List<ExprType> types = whenClauses.stream().map(WhenClause::type).collect(Collectors.toList());
    if (defaultResult != null) {
      types.add(defaultResult.type());
    }

    types.removeIf(type -> (type == UNDEFINED));
    return types;
  }

  private static List<Expression> concatArgs(
      List<WhenClause> whenClauses, Expression defaultResult) {
    ImmutableList.Builder<Expression> args = ImmutableList.builder();
    whenClauses.forEach(args::add);

    if (defaultResult != null) {
      args.add(defaultResult);
    }
    return args.build();
  }
}
