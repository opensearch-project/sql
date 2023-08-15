/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ComparisonTestTest.class,
  TestConfigTest.class,
  TestDataSetTest.class,
  TestQuerySetTest.class,
  TestReportTest.class,
  OpenSearchConnectionTest.class,
  JDBCConnectionTest.class,
  DBResultTest.class,
  RowTest.class,
})
public class UnitTests {}
