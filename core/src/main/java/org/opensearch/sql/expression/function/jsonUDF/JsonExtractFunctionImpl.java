/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior.NULL;
import static org.apache.calcite.sql.SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY;
import static org.opensearch.sql.calcite.utils.PPLReturnTypes.STRING_FORCE_NULLABLE;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.JsonFunctions;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class JsonExtractFunctionImpl extends ImplementorUDF {
  public JsonExtractFunctionImpl() {
    super(new JsonExtractImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class JsonExtractImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonExtractFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    if (args.length < 2) {
      return null;
    }
    JsonFunctions.StatefulFunction a = new JsonFunctions.StatefulFunction();
    String jsonStr = (String) args[0];
    List<Object> jsonPaths = Arrays.asList(args).subList(1, args.length);
    List<String> pathSpecs =
        jsonPaths.stream().map(Object::toString).map(JsonUtils::convertToJsonPath).toList();
    List<Object> results = new ArrayList<>();
    for (String pathSpec : pathSpecs) {
      Object queryResult = a.jsonQuery(jsonStr, pathSpec, WITHOUT_ARRAY, NULL, NULL, false);
      Object valueResult =
          a.jsonValue(
              jsonStr,
              pathSpec,
              SqlJsonValueEmptyOrErrorBehavior.NULL,
              null,
              SqlJsonValueEmptyOrErrorBehavior.NULL,
              null);
      results.add(queryResult != null ? queryResult : valueResult);
    }
    if (jsonPaths.size() == 1) {
      return doJsonize(results.getFirst());
    }
    return doJsonize(results);
  }

  private static boolean isScalarObject(Object obj) {
    if (obj instanceof Collection) {
      return false;
    } else {
      return !(obj instanceof Map);
    }
  }

  private static String doJsonize(Object candidate) {
    if (isScalarObject(candidate)) {
      return candidate.toString();
    } else {
      return JsonFunctions.jsonize(candidate);
    }
  }
}
