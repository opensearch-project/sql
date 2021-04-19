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
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;

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