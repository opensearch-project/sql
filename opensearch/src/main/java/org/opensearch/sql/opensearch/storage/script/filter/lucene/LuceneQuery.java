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

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Lucene query abstraction that builds Lucene query from function expression.
 */
public abstract class LuceneQuery {

  /**
   * Check if function expression supported by current Lucene query.
   * Default behavior is that report supported if:
   *  1. Left is a reference
   *  2. Right side is a literal
   *
   * @param func    function
   * @return        return true if supported, otherwise false.
   */
  public boolean canSupport(FunctionExpression func) {
    boolean multiParameterQuery = true;
    for (Expression expr : func.getArguments()) {
      multiParameterQuery = multiParameterQuery && expr instanceof NamedArgumentExpression;
    }
    return (func.getArguments().size() == 2)
        && (func.getArguments().get(0) instanceof ReferenceExpression)
        && (func.getArguments().get(1) instanceof LiteralExpression)
        || multiParameterQuery;
  }

  /**
   * Build Lucene query from function expression.
   *
   * @param func  function
   * @return      query
   */
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
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

}
