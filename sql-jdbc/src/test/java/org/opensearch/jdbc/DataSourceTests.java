/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc;

import org.opensearch.jdbc.auth.AuthenticationType;
import org.opensearch.jdbc.config.AuthConnectionProperty;
import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.config.HostConnectionProperty;
import org.opensearch.jdbc.config.LoginTimeoutConnectionProperty;
import org.opensearch.jdbc.config.PasswordConnectionProperty;
import org.opensearch.jdbc.config.PortConnectionProperty;
import org.opensearch.jdbc.config.UserConnectionProperty;
import org.opensearch.jdbc.test.PerTestWireMockServerExtension;
import org.opensearch.jdbc.test.WireMockServerHelpers;
import org.opensearch.jdbc.test.mocks.QueryMock;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(PerTestWireMockServerExtension.class)
public class DataSourceTests implements WireMockServerHelpers {

    @Test
    void testDataSourceConfig() throws SQLException {
        OpenSearchDataSource eds = new OpenSearchDataSource();

        Properties props = new Properties();
        props.setProperty(HostConnectionProperty.KEY, "some-host");
        props.setProperty(PortConnectionProperty.KEY, "1100");
        props.setProperty(LoginTimeoutConnectionProperty.KEY, "100");
        eds.setProperties(props);

        ConnectionConfig config = eds.getConnectionConfig(null);

        assertEquals("some-host", config.getHost());
        assertEquals(1100, config.getPort());
        assertEquals(100, config.getLoginTimeout());
        assertNull(config.getUser());
        assertNull(config.getPassword());
        Assertions.assertEquals(AuthenticationType.NONE, config.getAuthenticationType());
        assertNull(config.getAwsCredentialsProvider());
    }

    @Test
    void testDataSourceConfigWithDefaults() throws SQLException {
        OpenSearchDataSource eds = new OpenSearchDataSource();

        Properties defaults = new Properties();
        defaults.setProperty(UserConnectionProperty.KEY, "default-user");
        defaults.setProperty(PasswordConnectionProperty.KEY, "default-pass");
        defaults.setProperty(AuthConnectionProperty.KEY, "basic");

        Properties props = new Properties(defaults);
        props.setProperty(HostConnectionProperty.KEY, "some-host");
        props.setProperty(PortConnectionProperty.KEY, "1100");
        props.setProperty(LoginTimeoutConnectionProperty.KEY, "100");

        eds.setProperties(props);

        ConnectionConfig config = eds.getConnectionConfig(null);

        assertEquals("some-host", config.getHost());
        assertEquals(1100, config.getPort());
        assertEquals(100, config.getLoginTimeout());
        assertEquals("default-user", config.getUser());
        assertEquals("default-pass", config.getPassword());
        assertEquals(AuthenticationType.BASIC, config.getAuthenticationType());
    }

    @Test
    void testDataSourceConfigUpdate() throws SQLException {
        OpenSearchDataSource eds = new OpenSearchDataSource();
        Properties props = new Properties();
        props.setProperty(HostConnectionProperty.KEY, "some-host");
        props.setProperty(PortConnectionProperty.KEY, "1100");
        props.setProperty(LoginTimeoutConnectionProperty.KEY, "100");
        eds.setProperties(props);

        props = new Properties();
        props.setProperty(HostConnectionProperty.KEY, "some-host-updated");
        props.setProperty(PortConnectionProperty.KEY, "2100");
        eds.setProperties(props);

        ConnectionConfig config = eds.getConnectionConfig(null);

        assertEquals("some-host-updated", config.getHost());
        assertEquals(2100, config.getPort());
        assertEquals(0, config.getLoginTimeout());
        assertNull(config.getUser());
        assertNull(config.getPassword());
    }

    @Test
    void testDataSourceConfigUpdateWithOverrides() throws SQLException {
        OpenSearchDataSource eds = new OpenSearchDataSource();
        Properties props = new Properties();
        props.setProperty(HostConnectionProperty.KEY, "some-host");
        props.setProperty(PortConnectionProperty.KEY, "2100");
        eds.setProperties(props);

        Map<String, Object> overrides = new HashMap<>();
        overrides.put(UserConnectionProperty.KEY, "override-user");
        overrides.put(PasswordConnectionProperty.KEY, "override-pass");
        ConnectionConfig config = eds.getConnectionConfig(overrides);

        assertEquals("some-host", config.getHost());
        assertEquals(2100, config.getPort());
        assertEquals(0, config.getLoginTimeout());
        assertEquals("override-user", config.getUser());
        assertEquals("override-pass", config.getPassword());
    }

    @Test
    void testDataSourceConfigUpdateWithOverridesPrecedence() throws SQLException {
        OpenSearchDataSource eds = new OpenSearchDataSource();
        Properties props = new Properties();
        props.setProperty(HostConnectionProperty.KEY, "some-host");
        props.setProperty(PortConnectionProperty.KEY, "1100");
        props.setProperty(LoginTimeoutConnectionProperty.KEY, "100");
        eds.setProperties(props);

        props = new Properties();
        props.setProperty(HostConnectionProperty.KEY, "some-host-updated");
        props.setProperty(PortConnectionProperty.KEY, "2100");
        props.setProperty(UserConnectionProperty.KEY, "user");
        props.setProperty(PasswordConnectionProperty.KEY, "pass");
        eds.setProperties(props);

        ConnectionConfig config = eds.getConnectionConfig(null);

        assertEquals("some-host-updated", config.getHost());
        assertEquals(2100, config.getPort());
        assertEquals(0, config.getLoginTimeout());
        assertEquals("user", config.getUser());
        assertEquals("pass", config.getPassword());

        Map<String, Object> overrides = new HashMap<>();
        overrides.put(UserConnectionProperty.KEY, "override-user");
        overrides.put(PasswordConnectionProperty.KEY, "override-pass");
        config = eds.getConnectionConfig(overrides);

        assertEquals("some-host-updated", config.getHost());
        assertEquals(2100, config.getPort());
        assertEquals(0, config.getLoginTimeout());
        assertEquals("override-user", config.getUser());
        assertEquals("override-pass", config.getPassword());
    }

    @Test
    void testDataSourceFromUrlNycTaxisQuery(WireMockServer mockServer) throws SQLException, IOException {
        QueryMock queryMock = new QueryMock.NycTaxisQueryMock();
        queryMock.setupMockServerStub(mockServer);

        DataSource ds = new OpenSearchDataSource();
        ((OpenSearchDataSource) ds).setUrl(getBaseURLForMockServer(mockServer));

        Connection con = ds.getConnection();
        Statement st = con.createStatement();
        ResultSet rs = assertDoesNotThrow(() -> st.executeQuery(queryMock.getSql()));

        assertNotNull(rs);
        queryMock.getMockResultSet().assertMatches(rs);
    }

    @Test
    void testDataSourceFromPropsNycTaxisQuery(WireMockServer mockServer) throws SQLException, IOException {
        QueryMock queryMock = new QueryMock.NycTaxisQueryMock();
        queryMock.setupMockServerStub(mockServer);

        DataSource ds = new OpenSearchDataSource();
        ((OpenSearchDataSource) ds).setProperties(getConnectionPropertiesForMockServer(mockServer));

        Connection con = ds.getConnection();
        Statement st = con.createStatement();
        ResultSet rs = assertDoesNotThrow(() -> st.executeQuery(queryMock.getSql()));

        assertNotNull(rs);
        queryMock.getMockResultSet().assertMatches(rs);
    }
}
