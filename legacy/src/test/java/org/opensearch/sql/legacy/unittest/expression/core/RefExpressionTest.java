/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.expression.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.ref;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.doubleValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.integerValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.stringValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getBooleanValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getCollectionValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getDoubleValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getIntegerValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getStringValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getTupleValue;

import org.junit.Test;

public class RefExpressionTest extends ExpressionTest {
  @Test
  public void refIntegerValueShouldPass() {
    assertEquals(Integer.valueOf(1), getIntegerValue(ref("intValue").valueOf(bindingTuple())));
  }

  @Test
  public void refDoubleValueShouldPass() {
    assertEquals(Double.valueOf(2d), getDoubleValue(ref("doubleValue").valueOf(bindingTuple())));
  }

  @Test
  public void refStringValueShouldPass() {
    assertEquals("string", getStringValue(ref("stringValue").valueOf(bindingTuple())));
  }

  @Test
  public void refBooleanValueShouldPass() {
    assertEquals(true, getBooleanValue(ref("booleanValue").valueOf(bindingTuple())));
  }

  @Test
  public void refTupleValueShouldPass() {
    assertThat(
        getTupleValue(ref("tupleValue").valueOf(bindingTuple())),
        allOf(
            hasEntry("intValue", integerValue(1)),
            hasEntry("doubleValue", doubleValue(2d)),
            hasEntry("stringValue", stringValue("string"))));
  }

  @Test
  public void refCollectValueShouldPass() {
    assertThat(
        getCollectionValue(ref("collectValue").valueOf(bindingTuple())),
        contains(integerValue(1), integerValue(2), integerValue(3)));
  }
}
