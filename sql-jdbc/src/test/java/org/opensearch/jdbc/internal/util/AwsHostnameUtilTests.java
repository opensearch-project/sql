/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.internal.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class AwsHostnameUtilTests {

    /**
     * Test region name extracted from input hostname is as expected
     * when the input hostname is a known url format.
     *
     * @param hostname hostname to parse
     * @param expectedRegion expected region value
     */
    @ParameterizedTest
    @CsvSource({
            "search-domain-name.us-east-1.es.amazonaws.com, us-east-1",
            "search-domain-name.us-gov-west-1.es.amazonaws.com, us-gov-west-1",
            "search-domain-name.ap-southeast-2.es.a9.com, ap-southeast-2",
            "search-domain-name.sub-domain.us-west-2.es.amazonaws.com, us-west-2",
            "search-us-east-1.us-west-2.es.amazonaws.com, us-west-2",
    })
    void testNonNullRegionsFromAwsHostnames(String hostname, String expectedRegion) {
        assertEquals(expectedRegion, AwsHostNameUtil.parseRegion(hostname));
    }

    /**
     * Verify that a region value is not extracted from an input hostname
     *
     * @param hostname hostname to parse
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "search-domain-name.us-east-1.es.amazonaws.co",
            "search-domain-name.us-gov-west-1.es.amazonaws",
            "search-domain-name.ap-southeast-2.es.com",
    })
    void testNullRegions(String hostname) {
        String region = AwsHostNameUtil.parseRegion(hostname);
        assertNull(region, () -> hostname + " returned non-null region: " + region);
    }

}
