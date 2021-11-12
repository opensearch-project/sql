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

package org.opensearch.jdbc;

import org.opensearch.jdbc.test.PerClassWireMockServerExtension;
import org.opensearch.jdbc.test.WireMockServerHelpers;
import org.opensearch.jdbc.test.mocks.MockOpenSearch;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(PerClassWireMockServerExtension.class)
public class DriverTests implements WireMockServerHelpers {

    @Test
    public void testConnect(WireMockServer mockServer) throws SQLException {
        mockServer.stubFor(get(urlEqualTo("/"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(MockOpenSearch.INSTANCE.getConnectionResponse())));

        Driver driver = new Driver();
        Connection con = assertDoesNotThrow(() -> driver.connect(
                getBaseURLForMockServer(mockServer), (Properties) null));

        assertConnectionOpen(con);
        MockOpenSearch.INSTANCE.assertMockOpenSearchConnectionResponse((OpenSearchConnection) con);
    }


    private void assertConnectionOpen(final Connection con) {
        boolean closed = assertDoesNotThrow(con::isClosed);
        assertTrue(!closed, "Connection is closed");
    }

}
