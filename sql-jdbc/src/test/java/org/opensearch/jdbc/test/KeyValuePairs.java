/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A Factory class for building key-value pair objects
 */
public class KeyValuePairs {

    public static StringKvp skvp(String key, String value) {
        return new StringKvp(key, value);
    }

    /**
     * Models a key-value pair where both key and value are Strings
     */
    public static class StringKvp extends AbstractMap.SimpleImmutableEntry<String, String> {

        public StringKvp(String key, String value) {
            super(key, value);
        }
    }

    public static Properties toProperties(final StringKvp... kvps) {
        Properties props = new Properties();
        Arrays.stream(kvps).forEach(kvp -> props.setProperty(kvp.getKey(), kvp.getValue()));
        return props;
    }

    public static Map<String, String> toMap(final StringKvp... kvps) {
        return Arrays.stream(kvps).collect(Collectors.toMap(StringKvp::getKey, StringKvp::getValue));
    }
}


