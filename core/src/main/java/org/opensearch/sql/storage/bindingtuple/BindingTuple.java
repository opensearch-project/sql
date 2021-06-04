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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.storage.bindingtuple;

import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * BindingTuple represents the a relationship between bindingName and ExprValue.
 * e.g. The operation output column name is bindingName, the value is the ExprValue.
 */
public abstract class BindingTuple implements Environment<Expression, ExprValue> {
  public static BindingTuple EMPTY = new BindingTuple() {
    @Override
    public ExprValue resolve(ReferenceExpression ref) {
      return ExprMissingValue.of();
    }
  };

  /**
   * Resolve {@link Expression} in the BindingTuple environment.
   */
  @Override
  public ExprValue resolve(Expression var) {
    if (var instanceof ReferenceExpression) {
      return resolve(((ReferenceExpression) var));
    } else {
      throw new ExpressionEvaluationException(String.format("can resolve expression: %s", var));
    }
  }

  /**
   * Resolve the {@link ReferenceExpression} in BindingTuple context.
   */
  public abstract ExprValue resolve(ReferenceExpression ref);
}
