/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.test.mocks;

import org.opensearch.jdbc.OpenSearchConnection;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Utility class for obtaining mocked OpenSearch responses for tests.
 */
public class MockOpenSearch {
    // can be turned into a mock that can serve OpenSearch version specific
    // responses
    public static final MockOpenSearch INSTANCE = new MockOpenSearch();

    private MockOpenSearch() {

    }

    public String getConnectionResponse() {
        return "{\n" +
                "  \"name\" : \"NniGzjJ\",\n" +
                "  \"cluster_name\" : \"c1\",\n" +
                "  \"cluster_uuid\" : \"JpZSfOJiSLOntGp0zljpVQ\",\n" +
                "  \"version\" : {\n" +
                "    \"number\" : \"6.3.1\",\n" +
                "    \"build_flavor\" : \"default\",\n" +
                "    \"build_type\" : \"zip\",\n" +
                "    \"build_hash\" : \"4736258\",\n" +
                "    \"build_date\" : \"2018-10-11T03:50:25.929309Z\",\n" +
                "    \"build_snapshot\" : true,\n" +
                "    \"lucene_version\" : \"7.3.1\",\n" +
                "    \"minimum_wire_compatibility_version\" : \"5.6.0\",\n" +
                "    \"minimum_index_compatibility_version\" : \"5.0.0\"\n" +
                "  },\n" +
                "  \"tagline\" : \"You Know, for Search\"\n" +
                "}";
    }

    public void assertMockOpenSearchConnectionResponse(OpenSearchConnection openSearchCon) throws SQLException {
        assertEquals("c1", openSearchCon.getClusterName());
        assertEquals("JpZSfOJiSLOntGp0zljpVQ", openSearchCon.getClusterUUID());

        assertNotNull(openSearchCon.getMetaData().getDatabaseProductVersion());
        assertEquals("6.3.1", openSearchCon.getMetaData().getDatabaseProductVersion());
        assertEquals(6, openSearchCon.getMetaData().getDatabaseMajorVersion());
        assertEquals(3, openSearchCon.getMetaData().getDatabaseMinorVersion());
    }
}
