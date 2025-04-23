/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.collectionUDF;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LambdaUtils {
  public static class SimpleLambda {
    private Expression expression;
    private List<String> inputVariables;

    public SimpleLambda(String lambda) {
      String[] lambdaParts = lambda.split("->");
      if (lambdaParts.length != 2) {
        throw new IllegalArgumentException("Invalid lambda expression format");
      }

      String parameter = lambdaParts[0].trim();
      expression = AviatorEvaluator.compile(lambdaParts[1].trim());
      inputVariables =
          Arrays.stream(parameter.split(",")).map(String::trim).collect(Collectors.toList());
    }

    public List<String> parameters() {
      return inputVariables;
    }

    public Object execute(Map<String, Object> variableMap) {
      return expression.execute(variableMap);
    }
  }
}
