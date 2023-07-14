/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;

@ExtendWith(MockitoExtension.class)
class OpenSearchErrorMessageTest {

  @Mock
  private OpenSearchException openSearchException;

  @Mock
  private SearchPhaseExecutionException searchPhaseExecutionException;

  @Mock
  private ShardSearchFailure shardSearchFailure;

  @Test
  public void fetchReason() {
    when(openSearchException.getMessage()).thenReturn("error");

    OpenSearchErrorMessage errorMessage =
        new OpenSearchErrorMessage(openSearchException, SERVICE_UNAVAILABLE.getStatus());
    assertEquals("Error occurred in OpenSearch engine: error", errorMessage.fetchReason());
  }

  @Test
  public void fetchDetailsWithOpenSearchException() {
    when(openSearchException.getDetailedMessage()).thenReturn("detail error");

    OpenSearchErrorMessage errorMessage =
        new OpenSearchErrorMessage(openSearchException, SERVICE_UNAVAILABLE.getStatus());
    assertEquals("detail error\n"
            + "For more details, please send request for "
            + "Json format to see the raw response from OpenSearch engine.",
        errorMessage.fetchDetails());
  }

  @Test
  public void fetchDetailsWithSearchPhaseExecutionException() {
    when(searchPhaseExecutionException.shardFailures())
        .thenReturn(new ShardSearchFailure[] {shardSearchFailure});
    when(shardSearchFailure.shardId()).thenReturn(1);
    when(shardSearchFailure.getCause()).thenReturn(new IllegalStateException("illegal state"));

    OpenSearchErrorMessage errorMessage =
        new OpenSearchErrorMessage(searchPhaseExecutionException,
            SERVICE_UNAVAILABLE.getStatus());
    assertEquals("Shard[1]: java.lang.IllegalStateException: illegal state\n"
            + "\n"
            + "For more details, please send request for Json format to see the "
            + "raw response from OpenSearch engine.",
        errorMessage.fetchDetails());
  }
}
