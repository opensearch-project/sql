package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprLongValue;

import java.util.zip.CRC32;

public class CRC32Function implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Object value = args[0];
        CRC32 crc = new CRC32();
        crc.update(value.toString().getBytes());
        return crc.getValue();
    }
}
