/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.textUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class locateFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 2 && args.length != 3) {
            return new IllegalArgumentException("Invalid number of arguments, locate function expects 2 or 3 arguments");
        }
        String stringText = (String) args[0];
        String targetText = (String) args[1];
        if (stringText == null || targetText == null) {
            return null;
        }
        if (args.length == 2) {
            return stringText.indexOf(targetText) + 1;
        } else {
            int fromPosition = (int) args[2];
            return stringText.indexOf(targetText, fromPosition - 1) + 1;
        }
    }
}
