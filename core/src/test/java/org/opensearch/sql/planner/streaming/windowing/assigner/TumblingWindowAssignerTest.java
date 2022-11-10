/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.expression.DSL.span;
import static org.opensearch.sql.expression.DSL.window;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.streaming.windowing.Window;

class TumblingWindowAssignerTest {

  @Test
  void testAssignNumericWindow() {
    assertWindowAssigner()
        .windowingBy(ref("age", INTEGER), literal(1000), "")
        .shouldAssign(window(0, 1000)).to(500)
        .shouldAssign(window(1000, 2000)).to(1999)
        .shouldAssign(window(2000, 3000)).to(2000);
  }

  @Test
  void testAssignTimeWindow() {
    assertWindowAssigner()
        .windowingBy(ref("timestamp", DATETIME), literal(10), "m")
        .shouldAssign(window("2022-11-07 00:00:00", "2022-11-07 00:10:00", DATETIME))
        .to("2022-11-07 00:03:45")
        .shouldAssign(window("2022-11-07 00:00:00", "2022-11-07 00:10:00", DATETIME))
        .to("2022-11-07 00:09:59")
        .shouldAssign(window("2022-11-07 00:10:00", "2022-11-07 00:20:00", DATETIME))
        .to("2022-11-07 00:10:01");
  }

  private static AssertionHelper assertWindowAssigner() {
    return new AssertionHelper();
  }

  private static class AssertionHelper {
    /** Window assigner to be tested. */
    private WindowAssigner assigner;

    /** Fields that include the value to be windowed. */
    private Expression field;

    /** Expected window. */
    private Window expected;

    public AssertionHelper windowingBy(Expression field, Expression value, String unit) {
      assigner = new TumblingWindowAssigner(span(field, value, unit));
      this.field = field;
      return this;
    }

    public AssertionHelper shouldAssign(Window expected) {
      this.expected = expected;
      return this;
    }

    public AssertionHelper to(int number) {
      ExprValue row = ExprValueUtils.tupleValue(ImmutableMap.of(
          field.toString(), integerValue(number)));
      assertEquals(Collections.singletonList(expected), assigner.assign(row));
      return this;
    }

    public AssertionHelper to(String datetime) {
      ExprValue row = ExprValueUtils.tupleValue(ImmutableMap.of(
          field.toString(), fromObjectValue(datetime, (ExprCoreType) field.type())));
      assertEquals(Collections.singletonList(expected), assigner.assign(row));
      return this;
    }
  }
}