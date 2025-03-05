/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class ModFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("At least two arguments are required");
        }

        // Get the two values
        Object mod0 = args[0];
        Object mod1 = args[1];

        // Handle numbers dynamically
        if (mod0 instanceof Integer && mod1 instanceof Integer) {
            return (Integer) mod0 % (Integer) mod1;
        } else if (mod0 instanceof Number && mod1 instanceof Number) {
            double num0 = ((Number) mod0).doubleValue();
            double num1 = ((Number) mod1).doubleValue();

            if (num1 == 0) {
                throw new ArithmeticException("Modulo by zero is not allowed");
            }

            return num0 % num1; // Handles both float and double cases
        } else {
            throw new IllegalArgumentException("Invalid argument types: Expected numeric values");
        }
    }
}