/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/**
 * Get the Euler's number. () -> DOUBLE
 */
public class EulerFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 0) {
            throw new IllegalArgumentException(
                    String.format("Euler function takes no argument, but got %d", args.length));
        }

        return Math.E;
    }
}
