/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.Expression;

class FunctionDSLimplWithPropertiesNoArgsTest extends FunctionDSLimplTestBase {

  @Override
  SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      getImplementationGenerator() {
    return FunctionDSL.implWithProperties(fp -> ANY, ANY_TYPE);
  }

  @Override
  List<Expression> getSampleArguments() {
    return List.of();
  }

  @Override
  String getExpected_toString() {
    return "sample()";
  }
}
