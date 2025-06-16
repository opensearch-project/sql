/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import inet.ipaddr.IPAddress;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.utils.IPUtils;

/** Expression IP Address Value. */
public class ExprIpValue extends AbstractExprValue {
  private final IPAddress value;

  public ExprIpValue(String addressString) {
    value = IPUtils.toAddress(addressString);
  }

  @Override
  public String value() {
    return value.toCanonicalString();
  }

  @Override
  public ExprIpValue valueForCalcite() {
    return this;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.IP;
  }

  @Override
  public int compare(ExprValue other) {
    return IPUtils.compare(value, ((ExprIpValue) other).value);
  }

  @Override
  public boolean equal(ExprValue other) {
    return compare(other) == 0;
  }

  @Override
  public String toString() {
    return String.format("IP %s", value());
  }

  @Override
  public IPAddress ipValue() {
    return value;
  }
}
