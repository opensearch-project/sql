/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.ast.expression.Cast.isCastFunction;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Lucene query abstraction that builds Lucene query from function expression.
 */
public abstract class LuceneQuery {

  /**
   * Check if function expression supported by current Lucene query.
   * Default behavior is that report supported if:
   *  1. First argument (left operand) is a reference or a reference in cast function
   *  2. Second argument (right operand) is a literal
   * For cast function case, it's assumed that all lucene queries subclassed can support
   * type conversion itself by OpenSearch DSL underlying.
   *
   * @param func    function
   * @return        return true if supported, otherwise false.
   */
  public boolean canSupport(FunctionExpression func) {
    return (func.getArguments().size() == 2)
        && (isFirstReference(func) || isFirstReferenceInCastFunction(func))
        && isSecondLiteral(func);
  }

  /**
   * Build Lucene query from function expression.
   *
   * @param func  function
   * @return      query
   */
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref;
    if (isFirstReferenceInCastFunction(func)) {
      ref = (ReferenceExpression) extractArgInCastFunction(func);
    } else {
      ref = (ReferenceExpression) func.getArguments().get(0);
    }

    LiteralExpression literal = (LiteralExpression) func.getArguments().get(1);
    return doBuild(ref.getAttr(), ref.type(), literal.valueOf(null));
  }

  /**
   * Build method that subclass implements by default which is to build query
   * from reference and literal in function arguments.
   *
   * @param fieldName   field name
   * @param fieldType   field type
   * @param literal     field value literal
   * @return            query
   */
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    throw new UnsupportedOperationException(
        "Subclass doesn't implement this and build method either");
  }

  /**
   * Convert multi-field text field name to its inner keyword field. The limitation and assumption
   * is that the keyword field name is always "keyword" which is true by default.
   *
   * @param fieldName   field name
   * @param fieldType   field type
   * @return            keyword field name for multi-field, otherwise original field name returned
   */
  protected String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType == OPENSEARCH_TEXT_KEYWORD) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }

  private boolean isFirstReference(FunctionExpression expr) {
    return expr.getArguments().get(0) instanceof ReferenceExpression;
  }

  private boolean isFirstReferenceInCastFunction(FunctionExpression expr) {
    if (expr.getArguments().get(0) instanceof FunctionExpression) {
      FunctionExpression firstArg = (FunctionExpression) expr.getArguments().get(0);
      return isCastFunction(firstArg.getFunctionName()) && isFirstReference(firstArg);
    }
    return false;
  }

  private boolean isSecondLiteral(FunctionExpression func) {
    return func.getArguments().get(1) instanceof LiteralExpression;
  }

  private Expression extractArgInCastFunction(FunctionExpression expr) {
    return ((FunctionExpression) expr.getArguments().get(0)).getArguments().get(0);
  }

}
