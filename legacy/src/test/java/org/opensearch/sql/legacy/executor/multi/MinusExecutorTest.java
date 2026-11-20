/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.multi;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.query.multi.MultiQueryRequestBuilder;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

public class MinusExecutorTest {

  private final Client client = mock(Client.class);
  private final MultiQueryRequestBuilder builder = mock(MultiQueryRequestBuilder.class);

  @Before
  public void setUp() throws Exception {
    OpenSearchSettings settings = mock(OpenSearchSettings.class);
    when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    LocalClusterState.state().setPluginSettings(settings);

    Select select = mock(Select.class);
    when(select.getIndexArr()).thenReturn(new String[] {"logs"});
    when(builder.getOriginalSelect(true)).thenReturn(select);
    when(builder.getOriginalSelect(false)).thenReturn(select);
    when(builder.getFirstTableFieldToAlias()).thenReturn(Collections.emptyMap());
    when(builder.getSecondTableFieldToAlias()).thenReturn(Collections.emptyMap());
  }

  @Test
  public void clientErrorIsRethrownInsteadOfSwallowed() throws Exception {
    OpenSearchSecurityException denied =
        new OpenSearchSecurityException(
            "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN);
    failPitCreationWith(denied);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, new MinusExecutor(client, builder)::run);

    assertSame(denied, ErrorMessageFactory.findClientErrorCause(thrown).orElseThrow());
    // No point in time was created, so there is none to delete
    verify(client, never()).execute(any(DeletePitAction.class), any(DeletePitRequest.class));
  }

  @Test
  public void serverErrorIsRethrownInsteadOfSwallowed() throws Exception {
    OpenSearchStatusException unavailable =
        new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE);
    failPitCreationWith(unavailable);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, new MinusExecutor(client, builder)::run);

    assertSame(unavailable, ErrorMessageFactory.unwrapCause(thrown));
    verify(client, never()).execute(any(DeletePitAction.class), any(DeletePitRequest.class));
  }

  private void failPitCreationWith(Exception failure) throws Exception {
    ActionFuture<CreatePitResponse> createFuture = mock(ActionFuture.class);
    when(createFuture.get()).thenThrow(new ExecutionException(failure));
    when(client.execute(any(CreatePitAction.class), any(CreatePitRequest.class)))
        .thenReturn(createFuture);
  }
}
