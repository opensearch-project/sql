/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Suite to run all doc tests in one shot for local testing
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    SqlRequestTest.class,
    SqlResponseTest.class,
    SqlRequestFormatTest.class,
    SqlResponseFormatTest.class,
    DocBuilderTest.class,
    RstDocumentTest.class,
    DataTableTest.class,
})
public class DocTestTests {
}
