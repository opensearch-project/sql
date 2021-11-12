/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class MaxAggregator extends Aggregator<MaxAggregator.MaxState> {

  public MaxAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.MAX.getName(), arguments, returnType);
  }

  @Override
  public MaxState create() {
    return new MaxState();
  }

  @Override
  protected MaxState iterate(ExprValue value, MaxState state) {
    state.max(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format("max(%s)", format(getArguments()));
  }

  protected static class MaxState implements AggregationState {
    private ExprValue maxResult;

    MaxState() {
      maxResult = LITERAL_NULL;
    }

    public void max(ExprValue value) {
      maxResult = maxResult.isNull() ? value
          : (maxResult.compareTo(value) > 0)
          ? maxResult : value;
    }

    @Override
    public ExprValue result() {
      return maxResult;
    }
  }
}
