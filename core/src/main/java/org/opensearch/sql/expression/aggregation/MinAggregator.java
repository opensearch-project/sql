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
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Set;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The minimum aggregator aggregate the value evaluated by the expression.
 * If the expression evaluated result is NULL or MISSING, then the result is NULL.
 */
public class MinAggregator extends Aggregator<MinAggregator.MinState> {

  public MinAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.MIN.getName(), arguments, returnType);
  }


  @Override
  public MinState create() {
    return new MinState();
  }

  @Override
  protected MinState iterate(ExprValue value, MinState state) {
    state.min(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format("min(%s)", format(getArguments()));
  }

  protected static class MinState implements AggregationState {
    private ExprValue minResult;

    MinState() {
      minResult = LITERAL_NULL;
    }

    public void min(ExprValue value) {
      minResult = minResult.isNull() ? value
          : (minResult.compareTo(value) < 0)
          ? minResult : value;
    }

    @Override
    public ExprValue result() {
      return minResult;
    }

    @Override
    public Set<ExprValue> distinctSet() {
      return Set.of();
    }
  }
}
