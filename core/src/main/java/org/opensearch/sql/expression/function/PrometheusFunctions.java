package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

@UtilityClass
public class PrometheusFunctions {

  /**
   * Add table functions specific to Prometheus to repository.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(queryRange());
  }

  private static FunctionResolver queryRange() {
    FunctionName functionName = FunctionName.of("query_range");
    FunctionSignature functionSignature =
        new FunctionSignature(functionName, List.of(STRING, LONG, LONG, LONG));
    FunctionBuilder functionBuilder = arguments -> new PrometheusFunction(functionName, arguments);
    return new DefaultFunctionResolver(functionName,
        ImmutableMap.of(functionSignature, functionBuilder));
  }


  public static class PrometheusFunction extends FunctionExpression {
    private final FunctionName functionName;
    private final List<Expression> arguments;

    /**
     * Required argument constructor.
     *
     * @param functionName name of the function
     * @param arguments    a list of expressions
     */
    public PrometheusFunction(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(String.format(
          "Prometheus defined function [%s] is only supported in Table Function.",
          functionName));
    }

    @Override
    public ExprType type() {
      return ExprCoreType.STRUCT;
    }

    @Override
    public String toString() {
      List<String> args = arguments.stream()
          .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
              .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
          .collect(Collectors.toList());
      return String.format("%s(%s)", functionName, String.join(", ", args));
    }
  }

}
