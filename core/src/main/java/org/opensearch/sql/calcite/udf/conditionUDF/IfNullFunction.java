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
}
