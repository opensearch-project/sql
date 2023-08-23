/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.expression.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.sql.legacy.expression.model.ExprValueFactory;
import org.opensearch.sql.legacy.expression.model.ExprValueUtils;

@RunWith(MockitoJUnitRunner.class)
public class ExprValueUtilsTest {
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void getIntegerValueWithIntegerExprValueShouldPass() {
    assertThat(ExprValueUtils.getIntegerValue(ExprValueFactory.integerValue(1)), equalTo(1));
  }

  @Test
  public void getDoubleValueWithIntegerExprValueShouldPass() {
    assertThat(ExprValueUtils.getDoubleValue(ExprValueFactory.integerValue(1)), equalTo(1d));
  }

  @Test
  public void getIntegerWithDoubleExprValueShouldPass() {
    assertThat(ExprValueUtils.getIntegerValue(ExprValueFactory.doubleValue(1d)), equalTo(1));
  }

  @Test
  public void getLongValueFromLongExprValueShouldPass() {
    assertThat(ExprValueUtils.getLongValue(ExprValueFactory.from(1L)), equalTo(1L));
  }

  @Test
  public void getIntegerValueFromStringExprValueShouldThrowException() {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("invalid to get NUMBER_VALUE from expr type of STRING_VALUE");

    ExprValueUtils.getIntegerValue(ExprValueFactory.stringValue("string"));
  }

  @Test
  public void getStringValueFromIntegerExprValueShouldThrowException() {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("invalid to get STRING_VALUE from expr type of INTEGER_VALUE");

    ExprValueUtils.getStringValue(ExprValueFactory.integerValue(1));
  }
}
