/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Semantic analyzer test suite to prepare mapping and avoid load from file every time. But Gradle
 * seems not work well with suite. So move common logic to test base class and keep this for quick
 * testing in IDE.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  SemanticAnalyzerBasicTest.class,
  SemanticAnalyzerConfigTest.class,
  SemanticAnalyzerFromClauseTest.class,
  SemanticAnalyzerIdentifierTest.class,
  SemanticAnalyzerScalarFunctionTest.class,
  SemanticAnalyzerESScalarFunctionTest.class,
  SemanticAnalyzerAggregateFunctionTest.class,
  SemanticAnalyzerOperatorTest.class,
  SemanticAnalyzerSubqueryTest.class,
  SemanticAnalyzerMultiQueryTest.class,
})
public class SemanticAnalyzerTests {}
