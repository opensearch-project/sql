/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class MetricMetadataTest {

    @Test
    public void testNoArgsConstructor() {
        MetricMetadata metadata = new MetricMetadata();

        assertNull(metadata.getType());
        assertNull(metadata.getHelp());
        assertNull(metadata.getUnit());
    }

    @Test
    public void testAllArgsConstructor() {
        MetricMetadata metadata = new MetricMetadata("counter", "A counter metric", "bytes");

        assertEquals("counter", metadata.getType());
        assertEquals("A counter metric", metadata.getHelp());
        assertEquals("bytes", metadata.getUnit());
    }

    @Test
    public void testSettersAndGetters() {
        MetricMetadata metadata = new MetricMetadata();

        metadata.setType("gauge");
        metadata.setHelp("A gauge metric");
        metadata.setUnit("seconds");

        assertEquals("gauge", metadata.getType());
        assertEquals("A gauge metric", metadata.getHelp());
        assertEquals("seconds", metadata.getUnit());
    }

    @Test
    public void testEqualsAndHashCode() {
        MetricMetadata metadata1 = new MetricMetadata("histogram", "A histogram metric", "ms");
        MetricMetadata metadata2 = new MetricMetadata("histogram", "A histogram metric", "ms");
        MetricMetadata metadata3 = new MetricMetadata("counter", "A counter metric", "ms");

        // Test equals
        assertEquals(metadata1, metadata2);
        assertNotEquals(metadata1, metadata3);
        assertNotEquals(metadata1, null);
        assertNotEquals(metadata1, "string");

        // Test hashCode
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
    }

    @Test
    public void testEqualsWithNullValues() {
        MetricMetadata metadata1 = new MetricMetadata(null, null, null);
        MetricMetadata metadata2 = new MetricMetadata(null, null, null);
        MetricMetadata metadata3 = new MetricMetadata("counter", null, null);

        assertEquals(metadata1, metadata2);
        assertNotEquals(metadata1, metadata3);
    }

    @Test
    public void testEqualsWithPartiallyNullValues() {
        MetricMetadata metadata1 = new MetricMetadata("counter", null, "bytes");
        MetricMetadata metadata2 = new MetricMetadata("counter", null, "bytes");
        MetricMetadata metadata3 = new MetricMetadata("counter", "help", "bytes");

        assertEquals(metadata1, metadata2);
        assertNotEquals(metadata1, metadata3);
    }

    @Test
    public void testJsonIgnoreUnknownProperties() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        String json = "{\"type\":\"counter\",\"help\":\"A counter\",\"unit\":\"bytes\",\"unknownField\":\"value\"}";

        MetricMetadata metadata = objectMapper.readValue(json, MetricMetadata.class);

        assertEquals("counter", metadata.getType());
        assertEquals("A counter", metadata.getHelp());
        assertEquals("bytes", metadata.getUnit());
    }

    @Test
    public void testJsonSerialization() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        MetricMetadata metadata = new MetricMetadata("gauge", "Memory usage", "MB");

        String json = objectMapper.writeValueAsString(metadata);
        assertNotNull(json);

        MetricMetadata deserialized = objectMapper.readValue(json, MetricMetadata.class);
        assertEquals(metadata, deserialized);
    }

    @Test
    public void testToString() {
        MetricMetadata metadata = new MetricMetadata("summary", "Request duration", "seconds");
        String toString = metadata.toString();
        assertNotNull(toString);
    }
}