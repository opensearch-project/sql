package org.opensearch.sql.calcite.udf.conditionUDF;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

import java.util.List;
import java.util.Objects;

public class IfNullFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Object conditionValue = args[0];
        Object defaultValue = args[1];
        if (Objects.isNull(conditionValue)) {
            return defaultValue;
        }
        return conditionValue;
    }

    public static SqlReturnTypeInference getReturnTypeInference() {
        return opBinding -> {
            RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            // Get argument types
            List<RelDataType> argTypes = opBinding.collectOperandTypes();

            if (argTypes.isEmpty()) {
                throw new IllegalArgumentException("Function requires at least one argument.");
            }

            // Infer return type based on the first argument type (Modify as needed)
            RelDataType firstArgType = argTypes.get(1);

            if (firstArgType.getSqlTypeName() == SqlTypeName.INTEGER) {
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            } else if (firstArgType.getSqlTypeName() == SqlTypeName.DOUBLE ||
                    firstArgType.getSqlTypeName() == SqlTypeName.FLOAT) {
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            } else {
                return typeFactory.createSqlType(SqlTypeName.ANY);
            }
        };
    }
}
