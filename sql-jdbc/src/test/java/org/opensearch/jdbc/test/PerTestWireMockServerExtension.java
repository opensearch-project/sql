/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.test;


import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Field;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;


/**
 * JUnit extension to inject a WireMockServer instance into a
 * {@link WireMockServer} parameter for a Test.
 *
 * Use this extension to create a new {@link WireMockServer}
 * instance for each Test in a class.
 *
 * The extension ensures:
 * <p>
 *     <li>
 *         Before each test, mock server is started and made available to the Test as a parameter.
 *         Note: if a test does not declare a {@link WireMockServer} parameter, no mock server
 *         instance is created.
 *     </li>
 *     <li>
 *         After the test execution, the mock server is stopped.
 *     </li>
 * </p>
 *
 *
 */
public class PerTestWireMockServerExtension implements AfterEachCallback, ParameterResolver {

    private WireMockServer mockServer;

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
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
        return createAndStartMockServer();
    }
}
