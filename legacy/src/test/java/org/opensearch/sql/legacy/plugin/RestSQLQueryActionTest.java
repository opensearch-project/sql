/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.plugin.RestSQLQueryAction.NOT_SUPPORTED_YET;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;

@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionTest {

  @Mock
  private ClusterService clusterService;

  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  @Mock
  private Settings settings;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void handleQueryThatCanSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(clusterService, settings);
    assertNotSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

  @Test
  public void handleExplainThatCanSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        EXPLAIN_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(clusterService, settings);
    assertNotSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

  @Test
  public void skipQueryThatNotSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject(
            "{\"query\": \"SELECT name FROM test1 JOIN test2 ON test1.name = test2.name\"}"),
        "SELECT name FROM test1 JOIN test2 ON test1.name = test2.name",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(clusterService, settings);
    assertSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

}
