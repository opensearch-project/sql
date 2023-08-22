/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import com.google.common.base.Objects;
import lombok.RequiredArgsConstructor;

/** Expression Number Value. */
@RequiredArgsConstructor
public abstract class AbstractExprNumberValue extends AbstractExprValue {
  private final Number value;

  @Override
  public boolean isNumber() {
    return true;
  }

  @Override
  public Byte byteValue() {
    return value.byteValue();
  }

  @Override
  public Short shortValue() {
    return value.shortValue();
  }

  @Override
  public Integer integerValue() {
    return value.intValue();
  }

  @Override
  public Long longValue() {
    return value.longValue();
  }

  @Override
  public Float floatValue() {
    return value.floatValue();
  }

  @Override
  public Double doubleValue() {
    return value.doubleValue();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
