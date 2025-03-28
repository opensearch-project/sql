package org.opensearch.sql.calcite.udf;

import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.transferDateRelatedTimeName;

public class UserDefinedFunctionValidator {
    public boolean validateFunction(String op, List<RexNode> argList) {
        List<SqlTypeName> sqlTypeNames = argList.stream()
                .map(UserDefinedFunctionUtils::transferDateRelatedTimeName)
                .toList();

    }

    public static boolean judgeArgumentList(List<SqlTypeName> inputTypes, List<SqlTypeName> candidate) {
        if (inputTypes.size() != candidate.size()) {
            return false;
        }
        for (int i = 0; i < inputTypes.size(); i++) {
            if (inputTypes.get(i) != candidate.get(i)) {
                return false;
            }
        }
        return true;
    }

    private static Set<SqlTypeName> stringRelated = new HashSet<>();
}
