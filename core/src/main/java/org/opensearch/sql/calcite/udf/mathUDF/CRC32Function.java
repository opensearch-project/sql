/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import java.util.zip.CRC32;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/**
 * Calculate a cyclic redundancy check value and returns a 32-bit unsigned value<br>
 * The supported signature of crc32 function is<br>
 * STRING -> LONG
 */
public class CRC32Function implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    String.format("CRC32 function requires exactly one argument, but got %d", args.length));
        }
        Object value = args[0];
        CRC32 crc = new CRC32();
        crc.update(value.toString().getBytes());
        return crc.getValue();
    }
}
