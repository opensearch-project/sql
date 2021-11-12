/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
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
