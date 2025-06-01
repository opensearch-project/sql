/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.config;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.client.DataSourceClientFactory;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.datasource.query.QueryHandlerRegistry;
import org.opensearch.sql.directquery.DirectQueryExecutorService;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.prometheus.query.PrometheusQueryHandler;

public class DirectQueryModule extends AbstractModule {

  @Override
  protected void configure() {}

  @Provides
  @Singleton
  public DataSourceClientFactory dataSourceClientFactory(
      DataSourceService dataSourceService, Settings settings) {
    return new DataSourceClientFactory(dataSourceService, settings);
  }

  @Provides
  @Singleton
  public List<QueryHandler<?>> queryHandlers() {
    List<QueryHandler<?>> handlers = new ArrayList<>();
    handlers.add(new PrometheusQueryHandler());
    return handlers;
  }

  @Provides
  @Singleton
  public QueryHandlerRegistry queryHandlerRegistry(List<QueryHandler<?>> queryHandlers) {
    return new QueryHandlerRegistry(queryHandlers);
  }

  @Provides
  @Singleton
  public DirectQueryExecutorService directQueryExecutorServiceImpl(
      DataSourceClientFactory clientFactory, QueryHandlerRegistry queryHandlerRegistry) {
    return (DirectQueryExecutorService)
        new DirectQueryExecutorServiceImpl(clientFactory, queryHandlerRegistry);
  }
}
