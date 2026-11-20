/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.join;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.query.join.JoinRequestBuilder;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

public class ElasticJoinExecutorTest {

  private final Client client = mock(Client.class);
  private final JoinRequestBuilder requestBuilder = mock(JoinRequestBuilder.class);

  @Before
  public void setUp() {
    OpenSearchSettings settings = mock(OpenSearchSettings.class);
    when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    LocalClusterState.state().setPluginSettings(settings);

    Select select = mock(Select.class);
    when(select.getIndexArr()).thenReturn(new String[] {"logs"});
    TableInJoinRequestBuilder table = mock(TableInJoinRequestBuilder.class);
    when(table.getOriginalSelect()).thenReturn(select);
    when(requestBuilder.getFirstTable()).thenReturn(table);
    when(requestBuilder.getSecondTable()).thenReturn(table);
  }

  @Test
  public void deniedPitCreationIsThrownWithoutDeletingAPit() throws Exception {
    OpenSearchSecurityException denied =
        new OpenSearchSecurityException(
            "no permissions for [indices:data/read/point_in_time/create]", RestStatus.FORBIDDEN);
    ActionFuture<CreatePitResponse> createFuture = mock(ActionFuture.class);
    when(createFuture.get()).thenThrow(new ExecutionException(denied));
    when(client.execute(any(CreatePitAction.class), any(CreatePitRequest.class)))
        .thenReturn(createFuture);

    ElasticJoinExecutor executor =
        new ElasticJoinExecutor(client, requestBuilder) {
          @Override
          protected List<SearchHit> innerRun() {
            return Collections.emptyList();
          }
        };
    IllegalStateException thrown = assertThrows(IllegalStateException.class, executor::run);

    assertSame(denied, ErrorMessageFactory.findClientErrorCause(thrown).orElseThrow());
    verify(client, never()).execute(any(DeletePitAction.class), any(DeletePitRequest.class));
  }
}
