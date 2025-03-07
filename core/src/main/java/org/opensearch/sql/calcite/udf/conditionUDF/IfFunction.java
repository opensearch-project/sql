/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.conditionUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class IfFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("IF expects three arguments");
        }
        Object condition = args[0];
        Object trueValue = args[1];
        Object falseValue = args[2];
        if (condition instanceof Boolean) {
            return (Boolean) condition ? trueValue : falseValue;
        }
        else {
            throw new IllegalArgumentException("IF expects the first argument to be boolean");
        }
    }
}