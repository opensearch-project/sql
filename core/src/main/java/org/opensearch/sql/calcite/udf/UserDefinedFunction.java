package org.opensearch.sql.calcite.udf;

public interface UserDefinedFunction {
  Object eval(Object... args);
}
