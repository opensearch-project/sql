/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.internal.util.UrlParser;
import org.opensearch.jdbc.logging.LoggingSource;
import org.opensearch.jdbc.internal.Version;
import org.opensearch.jdbc.logging.LoggerFactory;
import org.opensearch.jdbc.logging.NoOpLogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver, LoggingSource {

    //
    // Register with the DriverManager
    //
    static {
        try {
            java.sql.DriverManager.registerDriver(new Driver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        ConnectionConfig connectionConfig = ConnectionConfig.builder()
                .setUrl(url)
                .setProperties(info)
                .build();
        org.opensearch.jdbc.logging.Logger log = initLog(connectionConfig);
        log.debug(() -> logMessage("connect (%s, %s)", url, info == null ? "null" : info.toString()));
        log.debug(() -> logMessage("Opening connection using config: %s", connectionConfig));
        return new ConnectionImpl(connectionConfig, log);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return UrlParser.isAcceptable(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // TODO - implement this?
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return Version.Current.getMajor();
    }

    @Override
    public int getMinorVersion() {
        return Version.Current.getMinor();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    static org.opensearch.jdbc.logging.Logger initLog(ConnectionConfig connectionConfig) {
        // precedence:
        // 1. explicitly supplied logWriter
        // 2. logOutput property
        // 3. DriverManager logWriter
        if (connectionConfig.getLogWriter() != null) {

            return LoggerFactory.getLogger(connectionConfig.getLogWriter(), connectionConfig.getLogLevel());

        } else if (connectionConfig.getLogOutput() != null) {

            return LoggerFactory.getLogger(connectionConfig.getLogOutput(), connectionConfig.getLogLevel());

        } else if (DriverManager.getLogWriter() != null) {

            return LoggerFactory.getLogger(DriverManager.getLogWriter(), connectionConfig.getLogLevel());

        } else {

            return NoOpLogger.INSTANCE;
        }
    }

}
