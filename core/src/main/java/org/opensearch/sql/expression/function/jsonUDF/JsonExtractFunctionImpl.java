/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.VARCHAR_FORCE_NULLABLE;
import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.convertToJsonPath;

import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class JsonExtractFunctionImpl extends ImplementorUDF {
  public JsonExtractFunctionImpl() {
    super(new JsonExtractImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
      if (sqlOperatorBinding.collectOperandTypes().size() > 2) {
        return createArrayType(
                typeFactory,
                varcharType,
                true);
      } else  {
        return varcharType;
      }
    };
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
    Object value = args[0];
    List<Object> results = new ArrayList<>();
    List<Object> paths = Arrays.asList(args).subList(1, args.length);
    for (Object path: paths) {
      String jsonPath = convertToJsonPath(path.toString());
      try {
        Object result;
        if (value instanceof String) {
          result = JsonPath.read((String) value, jsonPath);
        } else {
          result = JsonPath.read(value, jsonPath);
        }
        result = result != null ? gson.toJson(result) : null;
        results.add(result);
      } catch (Exception e) {
        results.add(null);
      }
    }
    if (paths.size() > 1) {
      return results;
    }
    return results.get(0);
  }
}
