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

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Locale;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The average aggregator aggregate the value evaluated by the expression.
 * If the expression evaluated result is NULL or MISSING, then the result is NULL.
 */
public class AvgAggregator extends Aggregator<AvgAggregator.AvgState> {

  public AvgAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.AVG.getName(), arguments, returnType);
  }

  @Override
  public AvgState create() {
    return new AvgState();
  }

  @Override
  protected AvgState iterate(ExprValue value, AvgState state) {
    state.count++;
    state.total += ExprValueUtils.getDoubleValue(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "avg(%s)", format(getArguments()));
  }

  /**
   * Average State.
   */
  protected static class AvgState implements AggregationState {
    private int count;
    private double total;

    AvgState() {
      this.count = 0;
      this.total = 0d;
    }

    @Override
    public ExprValue result() {
      return count == 0 ? ExprNullValue.of() : ExprValueUtils.doubleValue(total / count);
    }
  }
}
