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

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QualifiedNameContext;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

/**
 * AST Having filter builder that builds HAVING clause condition expressions
 * and replace alias by original expression in SELECT clause.
 * The reason for this is it's hard to replace afterwards since UnresolvedExpression
 * is immutable.
 */
@RequiredArgsConstructor
public class AstHavingFilterBuilder extends AstExpressionBuilder {

  private final QuerySpecification querySpec;

  @Override
  public UnresolvedExpression visitQualifiedName(QualifiedNameContext ctx) {
    return replaceAlias(super.visitQualifiedName(ctx));
  }

  private UnresolvedExpression replaceAlias(UnresolvedExpression expr) {
    if (querySpec.isSelectAlias(expr)) {
      return querySpec.getSelectItemByAlias(expr);
    }
    return expr;
  }

}
