/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc;

import org.opensearch.jdbc.config.HostnameVerificationConnectionProperty;
import org.opensearch.jdbc.config.TrustSelfSignedConnectionProperty;
import org.opensearch.jdbc.test.TLSServer;
import org.opensearch.jdbc.test.TestResources;
import org.opensearch.jdbc.test.mocks.MockOpenSearch;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.TempDirectory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TempDirectory.class)
public class SSLHostnameVerificationTests {

    // WireMockServer has a problem initializing server on TLS from
    // a password protected JKS keystore. If the issue gets fixed,
    // these tests can use WireMockServer instead.

    static Server jettyServer;
    static String connectURL;

    @BeforeAll
    static void beforeAll(@TempDirectory.TempDir Path tempDir) throws Exception {

        // Start server with SSL enabled
        Path keyStoreFile = tempDir.resolve("keystore");
        TestResources.copyResourceToPath(TLSServer.SERVER_KEY_JKS_RESOURCE_NON_LOCALHOST, keyStoreFile);
        System.out.println("Copied keystore to: " + keyStoreFile.toAbsolutePath().toString());

        String host = "localhost";
        jettyServer = TLSServer.startSecureServer(host,
                keyStoreFile.toAbsolutePath().toString(),
                "changeit",
                "JKS",
                new TLSServer.MockOpenSearchConnectionHandler());

        connectURL = TLSServer.getBaseURLForConnect(jettyServer);
        System.out.println("Started on: " + connectURL);
    }

    @AfterAll
    static void afterAll() throws Exception {
        System.out.println("Stopping jetty");
        jettyServer.stop();
    }

    @Test
    void testTrustSelfSignedEnabledHostnameVerificationDisabled() throws Exception {
        Properties props = new Properties();
        props.setProperty(TrustSelfSignedConnectionProperty.KEY, "true");
        props.setProperty(HostnameVerificationConnectionProperty.KEY, "false");

        Connection con = Assertions.assertDoesNotThrow(() -> new Driver().connect(connectURL, props));

        MockOpenSearch.INSTANCE.assertMockOpenSearchConnectionResponse((OpenSearchConnection) con);
    }

    @Test
    void testTrustSelfSignedEnabledHostnameVerificationEnabled() throws Exception {
        Properties props = new Properties();
        props.setProperty(TrustSelfSignedConnectionProperty.KEY, "true");
        props.setProperty(HostnameVerificationConnectionProperty.KEY, "true");

        SQLException sqe = Assertions.assertThrows(SQLException.class, () -> new Driver().connect(connectURL, props));
        assertTrue(sqe.getMessage().contains("javax.net.ssl.SSLPeerUnverifiedException"));
    }

    @Test
    void testTrustSelfSignedEnabledHostnameVerificationDefault() throws Exception {
        Properties props = new Properties();
        props.setProperty(TrustSelfSignedConnectionProperty.KEY, "true");

        SQLException sqe = Assertions.assertThrows(SQLException.class, () -> new Driver().connect(connectURL, props));
        assertTrue(sqe.getMessage().contains("javax.net.ssl.SSLPeerUnverifiedException"));
    }

}

