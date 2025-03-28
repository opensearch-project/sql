package org.opensearch.sql.calcite.udf;

import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimestampFunction;
import org.opensearch.sql.calcite.udf.mathUDF.ModFunction;
import org.opensearch.sql.calcite.udf.textUDF.LocateFunction;
import org.opensearch.sql.calcite.udf.textUDF.ReplaceFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.transferDateRelatedTimeName;

public class UserDefinedFunctionValidator {
    public boolean validateFunction(String op, List<RexNode> argList) {
        List<SqlTypeName> sqlTypeNames = argList.stream()
                .map(UserDefinedFunctionUtils::transferDateRelatedTimeName)
                .toList();

        switch (op.toUpperCase(Locale.ROOT)) {
            case "LOCATE":
                return LocateFunction.validArgument(sqlTypeNames);
            case "REPLACE":
                return ReplaceFunction.validArgument(sqlTypeNames);
            case "MOD":
                return ModFunction.validArgument(sqlTypeNames);
            case "TIMESTAMP":
                return TimestampFunction.validArgument(sqlTypeNames);
            default:
                return true;
        }
    }

    public static boolean judgeArgumentList(List<SqlTypeName> inputTypes, List<?> candidates) {
        if (inputTypes.size() != candidates.size()) {
            return false;
        }
        for (int i = 0; i < inputTypes.size(); i++) {
            SqlTypeName inputType = inputTypes.get(i);
            Object candidate = candidates.get(i);
            if (candidate instanceof SqlTypeName) {
                SqlTypeName candidateType = (SqlTypeName) candidate;
                if (candidateType != inputType) {
                    return false;
                }
            } else if (candidate instanceof Set<?>) {
                if (!((Set<?>) candidate).contains(inputType)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static Set<SqlTypeName> StringRelated = Set.of(SqlTypeName.VARCHAR, SqlTypeName.CHAR);
    public static Set<SqlTypeName> NumberRelated = Set.of(SqlTypeName.INTEGER, SqlTypeName.DECIMAL, SqlTypeName.DOUBLE, SqlTypeName.FLOAT, SqlTypeName.BIGINT);
    public static Set<SqlTypeName> DateRelated = Set.of(SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP);
    public static Set<SqlTypeName> CanBeTransferredToDate = Set.of(SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR, SqlTypeName.CHAR);
}
