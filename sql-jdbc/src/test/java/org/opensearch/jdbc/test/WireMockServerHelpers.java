/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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

package org.opensearch.jdbc.test;

import org.opensearch.jdbc.config.HostConnectionProperty;
import org.opensearch.jdbc.config.PortConnectionProperty;
import org.opensearch.jdbc.internal.util.UrlParser;
import com.github.tomakehurst.wiremock.WireMockServer;

import java.util.Properties;

/**
 * Adds {@link WireMockServer} related utility methods for
 * to Tests.
 * <p>
 * Useful for Tests that use WireMockServer extensions.
 */
public interface WireMockServerHelpers {

    /**
     * Returns a Driver compatible JDBC connection URL that points to
     * the {@link WireMockServer} instance specified on a specified
     * context path.
     *
     * @param mockServer {@link WireMockServer} instance
     * @param contextPath context path to place in the URL
     *
     * @return connection URL String
     */
    default String getURLForMockServerWithContext(WireMockServer mockServer, String contextPath) {
        return getBaseURLForMockServer(mockServer) + contextPath;
    }

    /**
     * Returns a Driver compatible JDBC connection URL that points to
     * the {@link WireMockServer} instance specified.
     *
     * @param mockServer {@link WireMockServer} instance
     *
     * @return connection URL String
     */
    default String getBaseURLForMockServer(WireMockServer mockServer) {
        // Change this in case 'localhost' is not ok to use in
        // all environments
        return UrlParser.URL_PREFIX + "localhost:" + mockServer.port();
    }

    /**
     * Returns a {@link Properties} object populated with connection
     * properties needed to establish a connection to the
     * {@link WireMockServer} instance specified.
     *
     * @param mockServer {@link WireMockServer} instance
     *
     * @return Properties object
     */
    default Properties getConnectionPropertiesForMockServer(WireMockServer mockServer) {
        Properties properties = new Properties();

        properties.setProperty(HostConnectionProperty.KEY, "localhost");
        properties.setProperty(PortConnectionProperty.KEY, String.valueOf(mockServer.port()));

        return properties;
    }
}
