package org.opensearch.sql.calcite.udf.conditionUDF;

import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class NullIfFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object firstValue = args[0];
    Object secondValue = args[1];
    if (Objects.equals(firstValue, secondValue)) {
      return null;
    }
    return firstValue;
  }
}
