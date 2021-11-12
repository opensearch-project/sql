/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.test;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.TimeZone;

public class UTCTimeZoneTestExtension implements BeforeEachCallback, AfterEachCallback {

    TimeZone jvmDefaultTimeZone;

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        // restore JVM default timezone
        TimeZone.setDefault(jvmDefaultTimeZone);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        jvmDefaultTimeZone = TimeZone.getDefault();

        // test case inputs assume default TZ is UTC
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
}
