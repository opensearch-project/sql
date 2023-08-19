/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.expression.core;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.literal;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.ref;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.integerValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.stringValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperation;

@RunWith(MockitoJUnitRunner.class)
public class BinaryExpressionTest extends ExpressionTest {
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void addIntegerValueShouldPass() {
    assertEquals(2, apply(ScalarOperation.ADD, ref("intValue"), ref("intValue")));
  }

  @Test
  public void multipleAddIntegerValueShouldPass() {
    assertEquals(
        3,
        apply(
            ScalarOperation.ADD,
            ref("intValue"),
            of(ScalarOperation.ADD, ref("intValue"), ref("intValue"))));
  }

  @Test
  public void addDoubleValueShouldPass() {
    assertEquals(4d, apply(ScalarOperation.ADD, ref("doubleValue"), ref("doubleValue")));
  }

  @Test
  public void addDoubleAndIntegerShouldPass() {
    assertEquals(3d, apply(ScalarOperation.ADD, ref("doubleValue"), ref("intValue")));
  }

  @Test
  public void divideIntegerValueShouldPass() {
    assertEquals(0, apply(ScalarOperation.DIVIDE, ref("intValue"), ref("intValue2")));
  }

  @Test
  public void divideIntegerAndDoubleShouldPass() {
    assertEquals(0.5d, apply(ScalarOperation.DIVIDE, ref("intValue"), ref("doubleValue")));
  }

  @Test
  public void subtractIntAndDoubleShouldPass() {
    assertEquals(-1d, apply(ScalarOperation.SUBTRACT, ref("intValue"), ref("doubleValue")));
  }

  @Test
  public void multiplyIntAndDoubleShouldPass() {
    assertEquals(2d, apply(ScalarOperation.MULTIPLY, ref("intValue"), ref("doubleValue")));
  }

  @Test
  public void modulesIntAndDoubleShouldPass() {
    assertEquals(1d, apply(ScalarOperation.MODULES, ref("intValue"), ref("doubleValue")));
  }

  @Test
  public void addIntAndStringShouldPass() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("unexpected operation type: ADD(INTEGER_VALUE, STRING_VALUE)");

    assertEquals(
        2,
        apply(ScalarOperation.ADD, literal(integerValue(1)), literal(stringValue("stringValue"))));
  }
}
