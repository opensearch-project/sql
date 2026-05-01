/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin;

import org.opensearch.analytics.spi.AnalyticsFrontEndExtension;
import org.opensearch.analytics.spi.AnalyticsServices;
import org.opensearch.sql.plugin.rest.AnalyticsExecutorHolder;

/**
 * SPI consumer that publishes the analytics-engine services into {@link AnalyticsExecutorHolder}
 * when analytics-engine is installed.
 *
 * <p>Kept separate from {@link SQLPlugin} so that SQLPlugin's bytecode does not reference any
 * analytics-framework class. When analytics-engine is absent, OpenSearch never invokes {@code
 * ServiceLoader.load(AnalyticsFrontEndExtension.class)} (because no plugin defining the SPI is
 * present), so this class is never resolved and SQL boots without needing analytics-framework on
 * its runtime classpath.
 */
public class SQLAnalyticsFrontEndExtension implements AnalyticsFrontEndExtension {

  @Override
  public void setAnalyticsServices(AnalyticsServices services) {
    AnalyticsExecutorHolder.set(services.queryPlanExecutor(), services.schemaProvider());
  }
}
