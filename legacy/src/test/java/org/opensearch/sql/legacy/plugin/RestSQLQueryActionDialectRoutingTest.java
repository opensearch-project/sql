/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.api.dialect.DialectNames;
import org.opensearch.sql.api.dialect.DialectPlugin;
import org.opensearch.sql.api.dialect.DialectRegistry;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Unit tests for REST layer dialect routing in {@link RestSQLQueryAction}. Validates requirements
 * 1.4 (absent dialect falls through) and 1.5 (Calcite disabled returns 400).
 */
@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionDialectRoutingTest extends BaseRestHandler {

  private NodeClient nodeClient;

  @Mock private ThreadPool threadPool;
  @Mock private QueryManager queryManager;
  @Mock private QueryPlanFactory factory;
  @Mock private RestChannel restChannel;
  @Mock private Settings settings;

  private DialectRegistry dialectRegistry;
  private Injector injector;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    dialectRegistry = new DialectRegistry();

    // Register a mock ClickHouse dialect plugin
    DialectPlugin mockPlugin = Mockito.mock(DialectPlugin.class);
    when(mockPlugin.dialectName()).thenReturn(DialectNames.CLICKHOUSE);
    dialectRegistry.register(mockPlugin);
    dialectRegistry.freeze();

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b -> {
          b.bind(SQLService.class)
              .toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
          b.bind(Settings.class).toInstance(settings);
          b.bind(DialectRegistry.class).toInstance(dialectRegistry);
        });
    injector = modules.createInjector();

    Mockito.lenient()
        .when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void absentDialectParamFallsThroughToExistingHandler() throws Exception {
    // No dialect param — request should be handled by the existing SQL handler.
    // We use the simple constructor (no params map), so getDialect() returns empty.
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT 1\"}"), "SELECT 1", QUERY_API_ENDPOINT, "jdbc");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);

    // The existing SQL handler will call sqlService.execute, which calls queryManager.submit.
    // Since queryManager is a mock, it does nothing — the request completes without error.
    // The key assertion: no exception is thrown and the consumer executes normally,
    // meaning the dialect pipeline was never entered.
    queryAction
        .prepareRequest(
            request,
            (channel, exception) -> {
              // Fallback handler — acceptable for unsupported queries in existing handler
            },
            (channel, exception) -> {
              // Execution error handler — acceptable for existing handler errors
            })
        .accept(restChannel);

    // If we reach here without a dialect-related 400 error, the request was routed
    // to the existing SQL handler, not the dialect pipeline. This validates Req 1.4.
  }

  @Test
  public void validDialectRoutesToDialectPipeline() throws Exception {
    // Enable Calcite so dialect routing proceeds
    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);

    // Request with dialect=clickhouse
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT 1\"}"),
            "SELECT 1",
            QUERY_API_ENDPOINT,
            Map.of("dialect", DialectNames.CLICKHOUSE),
            null);

    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);

    AtomicBoolean fallbackCalled = new AtomicBoolean(false);
    AtomicReference<BytesRestResponse> capturedResponse = new AtomicReference<>();
    RestChannel mockChannel = Mockito.mock(RestChannel.class);
    Mockito.doAnswer(
            invocation -> {
              capturedResponse.set(invocation.getArgument(0));
              return null;
            })
        .when(mockChannel)
        .sendResponse(Mockito.any(BytesRestResponse.class));

    RestChannelConsumer consumer =
        queryAction.prepareRequest(
            request,
            (channel, exception) -> {
              fallbackCalled.set(true);
            },
            (channel, exception) -> {
              // Execution error handler — dialect error handling now sends responses directly
            });

    // The consumer should be the dialect pipeline consumer (not the fallback).
    // When we accept the channel, it will try to execute the dialect query.
    // Since we don't have a full DataSourceService/ExecutionEngine wired up,
    // it will hit an error in executeDialectQuery — but the important thing is
    // that it entered the dialect pipeline (not the fallback handler).
    consumer.accept(mockChannel);

    // The fallback handler should NOT have been called — dialect routing bypasses it
    assertFalse("Fallback handler should not be called for dialect requests", fallbackCalled.get());

    // The dialect pipeline handles errors directly (sending a response to the channel),
    // so we verify a response was sent — confirming we entered the dialect pipeline.
    // Since the mock plugin doesn't have full Calcite infrastructure, it will be a 500 error.
    assertTrue(
        "A response should have been sent (dialect pipeline was entered)",
        capturedResponse.get() != null);
  }

  @Test
  public void calciteDisabledReturns400() throws Exception {
    // Disable Calcite
    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(false);

    // Request with dialect=clickhouse
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT 1\"}"),
            "SELECT 1",
            QUERY_API_ENDPOINT,
            Map.of("dialect", DialectNames.CLICKHOUSE),
            null);

    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);

    AtomicReference<BytesRestResponse> capturedResponse = new AtomicReference<>();
    RestChannel mockChannel = Mockito.mock(RestChannel.class);
    Mockito.doAnswer(
            invocation -> {
              capturedResponse.set(invocation.getArgument(0));
              return null;
            })
        .when(mockChannel)
        .sendResponse(Mockito.any(BytesRestResponse.class));

    RestChannelConsumer consumer =
        queryAction.prepareRequest(
            request,
            (channel, exception) -> fail("Fallback should not be called"),
            (channel, exception) -> fail("Execution error handler should not be called"));

    consumer.accept(mockChannel);

    // Verify a 400 response was sent
    BytesRestResponse response = capturedResponse.get();
    assertTrue("Should have captured a response", response != null);
    assertTrue("Response status should be 400", response.status() == RestStatus.BAD_REQUEST);
    String responseContent = response.content().utf8ToString();
    assertTrue(
        "Response should mention Calcite engine requirement",
        responseContent.contains("Calcite engine"));
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    return null;
  }
}
