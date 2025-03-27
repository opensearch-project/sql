/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.*;

/**
 * This function is  to transfer UDT to calcite datetime types
 */

public class PreprocessForUDTFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Object candidate = args[0];
        if (Objects.isNull(candidate)) {
            return null;
        }
        SqlTypeName sqlTypeName = (SqlTypeName) args[1];
        Instant instant =  InstantUtils.fromStringExpr((String) candidate);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        switch (sqlTypeName) {
            case DATE:
                return SqlFunctions.toInt(java.sql.Date.valueOf(localDateTime.toLocalDate()));
            case TIMESTAMP:
                return SqlFunctions.toLong(java.sql.Timestamp.valueOf(localDateTime));
            case TIME:
                return SqlFunctions.toInt(java.sql.Time.valueOf(localDateTime.toLocalTime()));
            default:
                throw new IllegalArgumentException("Unsupported sql type: " + sqlTypeName);
        }
    }

    public static SqlReturnTypeInference getSqlReturnTypeInference(ExprType type) {
        return opBinding -> {
            RelDataType relDataType;
            switch (type) {
                case ExprCoreType.DATE:
                    relDataType = opBinding.getTypeFactory().createSqlType(SqlTypeName.DATE);
                    break;
                case ExprCoreType.TIME:
                    relDataType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIME);
                    break;
                case ExprCoreType.TIMESTAMP:
                    relDataType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported sql type: " + type);
            }
            return opBinding.getTypeFactory().createTypeWithNullability(relDataType, true);
        };
    }

    public static SqlTypeName getInputType(ExprType type) {
        switch (type) {
            case ExprCoreType.DATE:
                return SqlTypeName.DATE;
            case ExprCoreType.TIME:
                return SqlTypeName.TIME;
            case ExprCoreType.TIMESTAMP:
                return SqlTypeName.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unsupported sql type: " + type);
        }
    }
}
