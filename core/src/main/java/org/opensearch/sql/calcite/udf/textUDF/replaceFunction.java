/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.textUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/**
 * We don't use calcite built in replace since it uses replace instead of replaceAll
 */

public class replaceFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("replace Function requires 3 arguments, but current get: " + args.length);
        }
        for (int i = 0; i < args.length; i++) {
            if (!(args[i] instanceof String)) {
                throw new IllegalArgumentException("replace Function requires String arguments, but current get: " + args[i]);
            }
        }
        String baseValue = (String) args[0];
        String fromValue = (String) args[1];
        String toValue = (String) args[2];
        return baseValue.replace(fromValue, toValue);
    }
}
