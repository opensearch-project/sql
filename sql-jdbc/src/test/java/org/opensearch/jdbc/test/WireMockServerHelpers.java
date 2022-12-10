/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
