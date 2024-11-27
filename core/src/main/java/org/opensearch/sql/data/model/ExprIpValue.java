/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Expression IP Address Value. */
public class ExprIpValue extends AbstractExprValue {
  private final IPAddress value;

  private static final IPAddressStringParameters validationOptions =
      new IPAddressStringParameters.Builder()
          .allowEmpty(false)
          .allowMask(false)
          .allowPrefix(false)
          .setEmptyAsLoopback(false)
          .allow_inet_aton(false)
          .allowSingleSegment(false)
          .toParams();

  public ExprIpValue(String s) {
    try {
      IPAddress address = new IPAddressString(s, validationOptions).toAddress();
      value = address.isIPv4Convertible() ? address.toIPv4() : address;
    } catch (AddressStringException e) {
      final String errorFormatString = "IP address '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormatString, s, e.getMessage()));
    }
  }

  @Override
  public String value() {
    return value.toCanonicalString();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.IP;
  }

  @Override
  public int compare(ExprValue other) {
    return value.compareTo(((ExprIpValue) other).value);
  }

  @Override
  public boolean equal(ExprValue other) {
    return value.equals(((ExprIpValue) other).value);
  }

  @Override
  public String toString() {
    return String.format("IP %s", value());
  }
}
