/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;

import java.util.Arrays;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableFunction;

/**
 * Utility class to register the method signature for geoip( ) expression,
 * concreted reallocated to `opensearch` module, as this Ip location require
 * GeoSpatial Plugin runtime support.
 */
@UtilityClass
public class GeoIPFunctions {

  public void register(BuiltinFunctionRepository repository) {
    repository.register(geoIp());
  }

  /**
   * To register all method signatures related to geoip( ) expression under eval.
   * @return Resolver for geoip( ) expression.
   */
  private DefaultFunctionResolver geoIp() {
    return define(
        BuiltinFunctionName.GEOIP.getName(),
        openSearchImpl(BOOLEAN, Arrays.asList(STRING, STRING)),
        openSearchImpl(BOOLEAN, Arrays.asList(STRING, STRING, STRING)));
  }

  /**
   * Util method to generate probe implementation with given list of argument types,
   * with marker class `OpenSearchFunctionExpression` to annotate this is an OpenSearch specific expression.
   * @param returnType return type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      openSearchImpl(ExprType returnType, List<ExprType> args) {
    return functionName -> {
      FunctionSignature functionSignature = new FunctionSignature(functionName, args);
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new OpenSearchFunctionExpression(functionName, arguments, returnType);
      return Pair.of(functionSignature, functionBuilder);
    };
  }
}
