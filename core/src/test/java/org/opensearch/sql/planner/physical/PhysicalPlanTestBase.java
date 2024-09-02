/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.planner.SerializablePlan;

public class PhysicalPlanTestBase {

  protected static final List<ExprValue> countTestInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 1, "name", "a")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 2, "name", "b")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 3)))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 4, "name", "d")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 5, "name", "e")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 6, "name", "f")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 7, "name", "g")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 8)))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 9, "name", "i")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "j")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 11, "name", "k")))
          .build();

  protected static final List<ExprValue> joinTestInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-03"), "host", "h1", "errors", 2)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-04"), "host", "h1", "errors", 1)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-04"), "host", "h2", "errors", 10)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-06"), "host", "h1", "errors", 1)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-07"), "host", "h1", "errors", 6)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 12)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-08"), "host", "h1", "errors", 13)))
          .build();

  protected static final List<ExprValue> inputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "ip",
                      "209.160.24.63",
                      "action",
                      "GET",
                      "response",
                      200,
                      "referer",
                      "www.amazon.com")))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "ip",
                      "209.160.24.63",
                      "action",
                      "GET",
                      "response",
                      404,
                      "referer",
                      "www.amazon.com")))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "ip",
                      "112.111.162.4",
                      "action",
                      "GET",
                      "response",
                      200,
                      "referer",
                      "www.amazon.com")))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "ip",
                      "74.125.19.106",
                      "action",
                      "POST",
                      "response",
                      200,
                      "referer",
                      "www.google.com")))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of("ip", "74.125.19.106", "action", "POST", "response", 500)))
          .build();

  private static Map<String, ExprCoreType> typeMapping =
      new ImmutableMap.Builder<String, ExprCoreType>()
          .put("ip", ExprCoreType.STRING)
          .put("action", ExprCoreType.STRING)
          .put("response", ExprCoreType.INTEGER)
          .put("referer", ExprCoreType.STRING)
          .build();

  protected static final List<ExprValue> dateInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-03"),
                      "month", new ExprDateValue("2021-02-04"),
                      "quarter", new ExprTimestampValue("2021-01-01 12:25:02"),
                      "year", new ExprTimestampValue("2013-01-01 12:25:02"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-01"),
                      "month", new ExprDateValue("2021-03-17"),
                      "quarter", new ExprTimestampValue("2021-05-17 12:25:01"),
                      "year", new ExprTimestampValue("2021-01-01 12:25:02"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-04"),
                      "month", new ExprDateValue("2021-02-08"),
                      "quarter", new ExprTimestampValue("2021-06-08 12:25:02"),
                      "year", new ExprTimestampValue("2016-01-01 12:25:02"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-02"),
                      "month", new ExprDateValue("2020-12-12"),
                      "quarter", new ExprTimestampValue("2020-12-12 12:25:03"),
                      "year", new ExprTimestampValue("1999-01-01 12:25:02"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-01"),
                      "month", new ExprDateValue("2021-02-28"),
                      "quarter", new ExprTimestampValue("2020-09-28 12:25:01"),
                      "year", new ExprTimestampValue("2018-01-01 12:25:02"))))
          .build();

  protected static final List<ExprValue> datetimeInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "hour", new ExprTimeValue("17:17:00"),
                      "minute", new ExprTimestampValue("2020-12-31 23:54:12"),
                      "second", new ExprTimestampValue("2021-01-01 00:00:05"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "hour", new ExprTimeValue("18:17:00"),
                      "minute", new ExprTimestampValue("2021-01-01 00:05:12"),
                      "second", new ExprTimestampValue("2021-01-01 00:00:12"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "hour", new ExprTimeValue("17:15:00"),
                      "minute", new ExprTimestampValue("2021-01-01 00:03:12"),
                      "second", new ExprTimestampValue("2021-01-01 00:00:17"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "hour", new ExprTimeValue("19:01:00"),
                      "minute", new ExprTimestampValue("2021-01-01 00:02:12"),
                      "second", new ExprTimestampValue("2021-01-01 00:00:03"))))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "hour", new ExprTimeValue("18:50:00"),
                      "minute", new ExprTimestampValue("2021-01-01 00:00:12"),
                      "second", new ExprTimestampValue("2021-01-01 00:00:13"))))
          .build();

  protected static final List<ExprValue> numericInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of("integer", 2, "long", 2L, "float", 2F, "double", 2D)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of("integer", 1, "long", 1L, "float", 1F, "double", 1D)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of("integer", 5, "long", 5L, "float", 5F, "double", 5D)))
          .build();

  protected static final List<ExprValue> compoundInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-03"),
                      "region",
                      "iad",
                      "host",
                      "h1",
                      "errors",
                      2)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-03"),
                      "region",
                      "iad",
                      "host",
                      "h2",
                      "errors",
                      3)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-04"),
                      "region",
                      "iad",
                      "host",
                      "h1",
                      "errors",
                      1)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-04"),
                      "region",
                      "iad",
                      "host",
                      "h2",
                      "errors",
                      10)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-06"),
                      "region",
                      "iad",
                      "host",
                      "h1",
                      "errors",
                      1)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-07"),
                      "region",
                      "iad",
                      "host",
                      "h1",
                      "errors",
                      6)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day",
                      new ExprDateValue("2021-01-07"),
                      "region",
                      "iad",
                      "host",
                      "h2",
                      "errors",
                      8)))
          .build();

  protected Environment<Expression, ExprCoreType> typeEnv() {
    return var -> {
      if (var instanceof ReferenceExpression) {
        ReferenceExpression refExpr = (ReferenceExpression) var;
        if (typeMapping.containsKey(refExpr.getAttr())) {
          return typeMapping.get(refExpr.getAttr());
        }
      }
      throw new ExpressionEvaluationException("type resolved failed");
    };
  }

  protected List<ExprValue> execute(PhysicalPlan plan) {
    ImmutableList.Builder<ExprValue> builder = new ImmutableList.Builder<>();
    plan.open();
    while (plan.hasNext()) {
      builder.add(plan.next());
    }
    plan.close();
    return builder.build();
  }

  protected static PhysicalPlan testScan(List<ExprValue> inputs) {
    return new TestScan(inputs);
  }

  protected static class TestScan extends PhysicalPlan implements SerializablePlan {
    private final Iterator<ExprValue> iterator;

    public TestScan() {
      iterator = inputs.iterator();
    }

    public TestScan(List<ExprValue> inputs) {
      iterator = inputs.iterator();
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
      return null;
    }

    @Override
    public List<PhysicalPlan> getChild() {
      return ImmutableList.of();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public ExprValue next() {
      return iterator.next();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {}

    public boolean equals(final Object o) {
      return o == this || o.hashCode() == hashCode();
    }

    public int hashCode() {
      return 42;
    }
  }
}
