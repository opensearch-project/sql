/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
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
import org.opensearch.sql.expression.function.SerializableFunction;

import java.util.Arrays;
import java.util.List;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

/** Utility class that defines and registers IP functions. */
@UtilityClass
public class GeoIPFunctions {

  public void register(BuiltinFunctionRepository repository) {
    repository.register(geoIp());

  }

  private DefaultFunctionResolver geoIp() {
    return define(
            BuiltinFunctionName.GEOIP.getName(),
            openSearchImpl(BOOLEAN, Arrays.asList(STRING, STRING)),
            openSearchImpl(BOOLEAN, Arrays.asList(STRING, STRING, STRING)));
  }

  /**
   * Triple Function Implementation.
   *
   * @param returnType return type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>> openSearchImpl(
          ExprType returnType,
          List<ExprType> args) {
    return functionName -> {
      FunctionSignature functionSignature = new FunctionSignature(functionName, args);
      FunctionBuilder functionBuilder =
              (functionProperties, arguments) ->
                      new OpenSearchFunctionExpression(functionName, arguments, returnType);
      return Pair.of(functionSignature, functionBuilder);
    };
  }

}
