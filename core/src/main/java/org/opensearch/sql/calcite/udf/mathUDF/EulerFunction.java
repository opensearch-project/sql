/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class EulerFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        return Math.E;
    }
}