/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.IP;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import inet.ipaddr.IPAddress;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.utils.IPUtils;

/** Utility class that defines and registers IP functions. */
@UtilityClass
public class IPFunctions {

  public void register(BuiltinFunctionRepository repository) {
    repository.register(cidrmatch());
  }

  private DefaultFunctionResolver cidrmatch() {
    return define(
        BuiltinFunctionName.CIDRMATCH.getName(),
        impl(nullMissingHandling(IPFunctions::exprCidrMatch), BOOLEAN, IP, STRING));
  }

  /**
   * Returns whether the given IP address is within the specified inclusive CIDR IP address range.
   * Supports both IPv4 and IPv6 addresses.
   *
   * @param addressExprValue IP address (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
   * @param rangeExprValue IP address range string in CIDR notation (e.g. "198.51.100.0/24" or
   *     "2001:0db8::/32")
   * @return true if the address is in the range; otherwise false.
   * @throws SemanticCheckException if the address or range is not valid, or if they do not use the
   *     same version (IPv4 or IPv6).
   */
  public static ExprValue exprCidrMatch(ExprValue addressExprValue, ExprValue rangeExprValue) {

    IPAddress address = addressExprValue.ipValue();
    IPAddress range = IPUtils.toRange(rangeExprValue.stringValue());

    return (IPUtils.compare(address, range.getLower()) < 0)
            || (IPUtils.compare(address, range.getUpper()) > 0)
        ? ExprValueUtils.LITERAL_FALSE
        : ExprValueUtils.LITERAL_TRUE;
  }
}
