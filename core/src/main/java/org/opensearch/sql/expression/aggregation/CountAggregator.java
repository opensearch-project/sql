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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.aggregation.CountAggregator.CountState;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class CountAggregator extends Aggregator<CountState> {

  public CountAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.COUNT.getName(), arguments, returnType);
  }

  @Override
  public CountAggregator.CountState create() {
    return new CountState();
  }

  @Override
  protected CountState iterate(ExprValue value, CountState state) {
    state.count(value, distinct);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "count(%s)", format(getArguments()));
  }

  /**
   * Count State.
   */
  protected static class CountState implements AggregationState {
    private int count;
    private final Set<ExprValue> distinctValues = new HashSet<>();

    CountState() {
      this.count = 0;
    }

    public void count(ExprValue value, Boolean distinct) {
      if (distinct) {
        if (!duplicated(value)) {
          distinctValues.add(value);
          count++;
        }
      } else {
        count++;
      }
    }

    private boolean duplicated(ExprValue value) {
      for (ExprValue exprValue : distinctValues) {
        if (value.compareTo(exprValue) == 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public ExprValue result() {
      return ExprValueUtils.integerValue(count);
    }
  }
}
