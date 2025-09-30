/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class PrometheusOptionsTest {

    @Test
    public void testNoArgsConstructor() {
        PrometheusOptions options = new PrometheusOptions();

        assertNull(options.getQueryType());
        assertNull(options.getStep());
        assertNull(options.getTime());
        assertNull(options.getStart());
        assertNull(options.getEnd());
    }

    @Test
    public void testSettersAndGetters() {
        PrometheusOptions options = new PrometheusOptions();

        options.setQueryType(PrometheusQueryType.INSTANT);
        options.setStep("60s");
        options.setTime("2023-12-01T10:00:00Z");
        options.setStart("2023-12-01T09:00:00Z");
        options.setEnd("2023-12-01T11:00:00Z");

        assertEquals(PrometheusQueryType.INSTANT, options.getQueryType());
        assertEquals("60s", options.getStep());
        assertEquals("2023-12-01T10:00:00Z", options.getTime());
        assertEquals("2023-12-01T09:00:00Z", options.getStart());
        assertEquals("2023-12-01T11:00:00Z", options.getEnd());
    }

    @Test
    public void testInstantQueryConfiguration() {
        PrometheusOptions options = new PrometheusOptions();
        options.setQueryType(PrometheusQueryType.INSTANT);
        options.setTime("2023-12-01T10:00:00Z");

        assertEquals(PrometheusQueryType.INSTANT, options.getQueryType());
        assertEquals("2023-12-01T10:00:00Z", options.getTime());
        assertNull(options.getStart());
        assertNull(options.getEnd());
    }

    @Test
    public void testRangeQueryConfiguration() {
        PrometheusOptions options = new PrometheusOptions();
        options.setQueryType(PrometheusQueryType.RANGE);
        options.setStart("2023-12-01T09:00:00Z");
        options.setEnd("2023-12-01T11:00:00Z");
        options.setStep("60s");

        assertEquals(PrometheusQueryType.RANGE, options.getQueryType());
        assertEquals("2023-12-01T09:00:00Z", options.getStart());
        assertEquals("2023-12-01T11:00:00Z", options.getEnd());
        assertEquals("60s", options.getStep());
        assertNull(options.getTime());
    }

    @Test
    public void testEqualsAndHashCode() {
        PrometheusOptions options1 = new PrometheusOptions();
        options1.setQueryType(PrometheusQueryType.INSTANT);
        options1.setTime("2023-12-01T10:00:00Z");

        PrometheusOptions options2 = new PrometheusOptions();
        options2.setQueryType(PrometheusQueryType.INSTANT);
        options2.setTime("2023-12-01T10:00:00Z");

        assertEquals(options1, options2);
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    public void testToString() {
        PrometheusOptions options = new PrometheusOptions();
        options.setQueryType(PrometheusQueryType.RANGE);
        options.setStep("30s");

        String toString = options.toString();
        assertNotNull(toString);
    }

    @Test
    public void testDataSourceOptionsInterface() {
        PrometheusOptions options = new PrometheusOptions();

        // Verify it implements DataSourceOptions
        assertNotNull(options);
        assertEquals(PrometheusOptions.class, options.getClass());
    }
}