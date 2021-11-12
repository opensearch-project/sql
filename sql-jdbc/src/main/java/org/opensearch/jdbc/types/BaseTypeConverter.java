/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class BaseTypeConverter implements TypeConverter {

    static final Map<Class, TypeHelper> typeHandlerMap = new HashMap<>();

    static {
        typeHandlerMap.put(String.class, StringType.INSTANCE);

        typeHandlerMap.put(Byte.class, ByteType.INSTANCE);
        typeHandlerMap.put(Short.class, ShortType.INSTANCE);
        typeHandlerMap.put(Integer.class, IntegerType.INSTANCE);
        typeHandlerMap.put(Long.class, LongType.INSTANCE);

        typeHandlerMap.put(Float.class, FloatType.INSTANCE);
        typeHandlerMap.put(Double.class, DoubleType.INSTANCE);

        typeHandlerMap.put(Boolean.class, BooleanType.INSTANCE);

        typeHandlerMap.put(Timestamp.class, TimestampType.INSTANCE);
        typeHandlerMap.put(Date.class, DateType.INSTANCE);
        typeHandlerMap.put(Time.class, TimeType.INSTANCE);

    }

    @Override
    public <T> T convert(Object value, Class<T> clazz, Map<String, Object> conversionParams) throws SQLException {
        if (clazz == null) {
            clazz = getDefaultJavaClass();
        }

        if (getSupportedJavaClasses() != null && getSupportedJavaClasses().contains(clazz)) {
            TypeHelper<T> typeHelper = getTypeHelper(clazz);

            if (typeHelper != null) {
                return typeHelper.fromValue(value, conversionParams);
            }
        }
        throw objectConversionException(value, clazz);
    }

    private <T> TypeHelper<T> getTypeHelper(Class<T> clazz) {
        return typeHandlerMap.get(clazz);
    }

    public abstract Class getDefaultJavaClass();

    public abstract Set<Class> getSupportedJavaClasses();
}
