/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;

/** Utility class that defines and registers IP functions. */
@UtilityClass
public class IPFunction {

  public void register(BuiltinFunctionRepository repository) {
    repository.register(cidrmatch());
  }

  private DefaultFunctionResolver cidrmatch() {
    return define(
        BuiltinFunctionName.CIDRMATCH.getName(),
        impl(nullMissingHandling(IPFunction::exprCidrMatch), BOOLEAN, STRING, STRING));
  }

  /**
   * Returns whether the given IP address is within the specified CIDR IP address range. Supports
   * both IPv4 and IPv6 addresses.
   *
   * @param addressExprValue IP address (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
   * @param rangeExprValue IP address range in CIDR notation (e.g. "198.51.100.0/24" or
   *     "2001:0db8::/32")
   * @return true if the address is in the range; otherwise false.
   * @throws SemanticCheckException if the address or range is not valid, or if they do not use the
   *     same version (IPv4 or IPv6).
   */
  private ExprValue exprCidrMatch(ExprValue addressExprValue, ExprValue rangeExprValue) {

    String addressString = addressExprValue.stringValue();
    String rangeString = rangeExprValue.stringValue();

    final IPAddressStringParameters validationOptions =
        new IPAddressStringParameters.Builder()
            .allowEmpty(false)
            .setEmptyAsLoopback(false)
            .allow_inet_aton(false)
            .allowSingleSegment(false)
            .toParams();

    // Get and validate IP address.
    IPAddressString address =
        new IPAddressString(addressExprValue.stringValue(), validationOptions);

    try {
      address.validate();
    } catch (AddressStringException e) {
      String msg =
          String.format(
              "IP address '%s' is not valid. Error details: %s", addressString, e.getMessage());
      throw new SemanticCheckException(msg, e);
    }

    // Get and validate CIDR IP address range.
    IPAddressString range = new IPAddressString(rangeExprValue.stringValue(), validationOptions);

    try {
      range.validate();
    } catch (AddressStringException e) {
      String msg =
          String.format(
              "CIDR IP address range '%s' is not valid. Error details: %s",
              rangeString, e.getMessage());
      throw new SemanticCheckException(msg, e);
    }

    // Address and range must use the same IP version (IPv4 or IPv6).
    if (address.isIPv4() ^ range.isIPv4()) {
      String msg =
          String.format(
              "IP address '%s' and CIDR IP address range '%s' are not compatible. Both must be"
                  + " either IPv4 or IPv6.",
              addressString, rangeString);
      throw new SemanticCheckException(msg);
    }

    return ExprValueUtils.booleanValue(range.contains(address));
  }
}
