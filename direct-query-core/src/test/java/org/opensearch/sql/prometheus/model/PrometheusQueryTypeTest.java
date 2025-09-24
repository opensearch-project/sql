/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class PrometheusQueryTypeTest {

    @Test
    public void testEnumValues() {
        assertEquals("instant", PrometheusQueryType.INSTANT.getValue());
        assertEquals("range", PrometheusQueryType.RANGE.getValue());
    }

    @Test
    public void testFromStringValidValues() {
        assertEquals(PrometheusQueryType.INSTANT, PrometheusQueryType.fromString("instant"));
        assertEquals(PrometheusQueryType.RANGE, PrometheusQueryType.fromString("range"));

        // Test case insensitive
        assertEquals(PrometheusQueryType.INSTANT, PrometheusQueryType.fromString("INSTANT"));
        assertEquals(PrometheusQueryType.RANGE, PrometheusQueryType.fromString("RANGE"));
        assertEquals(PrometheusQueryType.INSTANT, PrometheusQueryType.fromString("Instant"));
        assertEquals(PrometheusQueryType.RANGE, PrometheusQueryType.fromString("Range"));
    }

    @Test
    public void testFromStringInvalidValue() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> PrometheusQueryType.fromString("invalid")
        );
        assertEquals("Unknown query type: invalid", exception.getMessage());

        assertThrows(
            IllegalArgumentException.class,
            () -> PrometheusQueryType.fromString("")
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> PrometheusQueryType.fromString(null)
        );
    }

    @Test
    public void testToString() {
        assertEquals("instant", PrometheusQueryType.INSTANT.toString());
        assertEquals("range", PrometheusQueryType.RANGE.toString());
    }

    @Test
    public void testEnumComparison() {
        assertEquals(PrometheusQueryType.INSTANT, PrometheusQueryType.INSTANT);
        assertEquals(PrometheusQueryType.RANGE, PrometheusQueryType.RANGE);
    }
}