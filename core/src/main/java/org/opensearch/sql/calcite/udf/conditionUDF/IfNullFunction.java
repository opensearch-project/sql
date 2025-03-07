/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.conditionUDF;

import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class IfNullFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Null if function expects two arguments");
        }
        Object conditionValue = args[0];
        Object defaultValue = args[1];
        if (Objects.isNull(conditionValue)) {
            return defaultValue;
        }
        return conditionValue;
    }
}