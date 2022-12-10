/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.test;


import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;


/**
 * JUnit extension to inject a WireMockServer instance into a
 * {@link WireMockServer} parameter for a Test.
 * <p>
 * Use this extension to reuse a single {@link WireMockServer}
 * instance across all Tests in a class.
 * <p>
 * Since the tests operate on a shared mock server instance,
 * thread safety should be considered if any of the tests
 * are expected to be run in parallel.
 * <p>
 * The extension ensures:
 * <p>
 *     <li>
 *        Before any tests run, a mock server is started..
 *     </li>
 *     <li>
 *         Each Test declaring a {@link WireMockServer} parameter receives
 *         the mock server instance in the parameter.
 *     </li>
 *     <li>
 *         After each test, all request Stub mappings are reset - this
 *         ensures request mappings created in one test never leak into
 *         a subsequent test.
 *     </li>
 *     <li>
 *         After all tests, the mock server is stopped.
 *     </li>
 * </p>
 */
public class PerClassWireMockServerExtension implements BeforeAllCallback, AfterAllCallback,
        AfterEachCallback, ParameterResolver {

    private WireMockServer mockServer;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        createAndStartMockServer();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        mockServer.resetToDefaultMappings();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        cleanupMockServer(context);
    }

    private WireMockServer createAndStartMockServer() {
        System.out.println("Creating mock server");
        mockServer = new WireMockServer(options()
                .dynamicPort()
                .notifier(new ConsoleNotifier(true)
                ));

        mockServer.start();
        return mockServer;
    }

    private void cleanupMockServer(ExtensionContext context) {
        if (mockServer != null) {
            System.out.println("Cleaning up mock server");
            mockServer.stop();
            mockServer = null;
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == WireMockServer.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return mockServer;
    }
}
