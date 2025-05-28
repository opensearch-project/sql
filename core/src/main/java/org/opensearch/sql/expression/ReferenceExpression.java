/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ReferenceExpression implements Expression {
  @Getter private final String attr;

  @Getter private final String rawPath;

  @Getter private final List<String> paths;

  @Getter private final ExprType type;

  /**
   * Constructor of ReferenceExpression.
   *
   * @param ref the field name. e.g. addr.state/addr.
   * @param type type.
   */
  public ReferenceExpression(String ref, ExprType type) {
    this.attr = ref;
    // Todo. the define of paths need to be redefined after adding multiple index/variable support.
    // For AliasType, the actual path is set in the property of `path` and the type is derived
    // from the type of field on that path; Otherwise, use ref itself as the path
    this.rawPath = type.getOriginalPath().orElse(ref);
    this.paths = Arrays.asList(rawPath.split("\\."));
    this.type = type.getOriginalExprType();
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> env) {
    return env.resolve(this);
  }

  @Override
  public ExprType type() {
    return type;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitReference(this, context);
  }

  @Override
  public String toString() {
    return attr;
  }

  /**
   * <pre>
   * Resolve the ExprValue from {@link ExprTupleValue} using paths.
   * Considering the following sample data.
   * {
   *   "name": "bob smith"
   *   "project.year": 1990,
   *   "project": {
   *     "year": "2020"
   *   }
   *   "address": {
   *     "state": "WA",
   *     "city": "seattle",
   *     "project.year": 1990
   *   }
   *   "address.local": {
   *     "state": "WA",
   *   }
   * }
   * The paths could be
   * 1. top level, e.g. "name", which will be resolved as "bob smith"
   * 2. multiple paths, e.g. "name.address.state", which will be resolved as "WA"
   * 3. special case, the "." is the path separator, but it is possible that the path include
   * ".", for handling this use case, we define the resolve rule as bellow, e.g. "project.year" is
   * resolved as 1990 instead of 2020. Note. This logic only applied top level none object field.
   * e.g. "address.local.state" been resolved to Missing. but "address.project.year" could been
   * resolved as 1990.
   *
   * <p>Resolve Rule
   * 1. Resolve the full name by combine the paths("x"."y"."z") as whole ("x.y.z").
   * 2. Resolve the path recursively through ExprValue.
   *
   * @param value {@link ExprTupleValue}.
   * @return {@link ExprTupleValue}.
   * </pre>
   */
  public ExprValue resolve(ExprTupleValue value) {
    return ExprValueUtils.resolveRefPaths(value, paths);
  }
}
