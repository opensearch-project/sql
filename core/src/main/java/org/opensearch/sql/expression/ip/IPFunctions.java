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
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.expression.function.SerializableFunction;
import org.opensearch.sql.utils.IPUtils;

import java.util.Arrays;
import java.util.List;

/** Utility class that defines and registers IP functions. */
@UtilityClass
public class IPFunctions {

  public void register(BuiltinFunctionRepository repository) {
    repository.register(cidrmatch());
    repository.register(geoIp());
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
  private ExprValue exprCidrMatch(ExprValue addressExprValue, ExprValue rangeExprValue) {

    IPAddress address = addressExprValue.ipValue();
    IPAddress range = IPUtils.toRange(rangeExprValue.stringValue());

    return (IPUtils.compare(address, range.getLower()) < 0)
            || (IPUtils.compare(address, range.getUpper()) > 0)
        ? ExprValueUtils.LITERAL_FALSE
        : ExprValueUtils.LITERAL_TRUE;
  }

  /**
   * To register all method signatures related to geoip( ) expression under eval.
   *
   * @return Resolver for geoip( ) expression.
   */
  private DefaultFunctionResolver geoIp() {
    return define(
            BuiltinFunctionName.GEOIP.getName(),
            openSearchImpl(BOOLEAN, Arrays.asList(STRING, STRING)),
            openSearchImpl(BOOLEAN, Arrays.asList(STRING, STRING, STRING)));
  }

  /**
   * Util method to generate probe implementation with given list of argument types, with marker
   * class `OpenSearchFunction` to annotate this is an OpenSearch specific expression.
   *
   * @param returnType return type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
  openSearchImpl(ExprType returnType, List<ExprType> args) {
    return functionName -> {
      FunctionSignature functionSignature = new FunctionSignature(functionName, args);
      FunctionBuilder functionBuilder =
              (functionProperties, arguments) ->
                      new OpenSearchFunctions.OpenSearchFunction(functionName, arguments, returnType);
      return Pair.of(functionSignature, functionBuilder);
    };
  }
}
