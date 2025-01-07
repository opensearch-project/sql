/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Simple http server to serve static files under test/java/resources/ip2geo/server for integration testing
 */
@Log4j2
@SuppressForbidden(reason = "used only for testing")
public class Ip2GeoDataServer {
    private static final String SYS_PROPERTY_KEY_CLUSTER_ENDPOINT = "tests.rest.cluster";
    private static final String LOCAL_CLUSTER_ENDPOINT = "127.0.0.1";
    private static final String ROOT = "ip2geo/server";
    private static final int PORT = 8001;
    private static final String EXTERNAL_ENDPOINT_PREFIX =
        "https://raw.githubusercontent.com/opensearch-project/geospatial/main/src/test/resources/ip2geo/server";

    private static HttpServer server;
    private static volatile int counter = 0;
    private static String endpointPrefix = "http://localhost:" + PORT;
    private static String cityFilePath = endpointPrefix + "/city/manifest_local.json";
    private static String countryFilePath = endpointPrefix + "/country/manifest_local.json";

    /**
     * Return an endpoint to a manifest file for a sample city data
     * The sample data should contain three lines as follows
     *
     * cidr,city,country
     * 10.0.0.0/8,Seattle,USA
     * 127.0.0.0/12,Vancouver,Canada
     * fd12:2345:6789:1::/64,Bengaluru,India
     *
     */
    public static String getEndpointCity() {
        return cityFilePath;
    }

    /**
     * Return an endpoint to a manifest file for a sample country data
     * The sample data should contain three lines as follows
     *
     * cidr,country
     * 10.0.0.0/8,USA
     * 127.0.0.0/12,Canada
     * fd12:2345:6789:1::/64,India
     *
     */
    public static String getEndpointCountry() {
        return countryFilePath;
    }

    @SneakyThrows
    synchronized public static void start() {
        log.info("Start server is called");
        // If it is remote cluster test, use external endpoint and do not launch local server
        if (System.getProperty(SYS_PROPERTY_KEY_CLUSTER_ENDPOINT).contains(LOCAL_CLUSTER_ENDPOINT) == false) {
            log.info("Remote cluster[{}] testing. Skip launching local server", System.getProperty(SYS_PROPERTY_KEY_CLUSTER_ENDPOINT));
            cityFilePath = EXTERNAL_ENDPOINT_PREFIX + "/city/manifest.json";
            countryFilePath = EXTERNAL_ENDPOINT_PREFIX + "/country/manifest.json";
            return;
        }

        counter++;
        if (server != null) {
            log.info("Server has started already");
            return;
        }
        server = HttpServer.create(new InetSocketAddress("localhost", PORT), 0);
        server.createContext("/", new Ip2GeoHttpHandler());
        server.start();
        log.info("Local file server started on port {}", PORT);
    }

    synchronized public static void stop() {
        log.info("Stop server is called");
        if (server == null) {
            log.info("Server has stopped already");
            return;
        }
        counter--;
        if (counter > 0) {
            log.info("[{}] processors are still using the server", counter);
            return;
        }

        server.stop(0);
        server = null;
        log.info("Server stopped");
    }

    @SuppressForbidden(reason = "used only for testing")
    private static class Ip2GeoHttpHandler implements HttpHandler {
        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            try {
                byte[] data = Files.readAllBytes(
                    Paths.get(this.getClass().getClassLoader().getResource(ROOT + exchange.getRequestURI().getPath()).toURI())
                );
                exchange.sendResponseHeaders(200, data.length);
                OutputStream outputStream = exchange.getResponseBody();
                outputStream.write(data);
                outputStream.flush();
                outputStream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
