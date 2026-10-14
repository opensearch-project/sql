/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Test;
import org.opensearch.rest.RestChannel;
import org.opensearch.transport.client.node.NodeClient;

public class SqlCoordinatorTaskDispatcherTest {

  @Test
  public void passthroughRunsWorkWithGivenChannel() {
    NodeClient client = mock(NodeClient.class);
    RestChannel channel = mock(RestChannel.class);
    AtomicBoolean ran = new AtomicBoolean(false);
    AtomicReference<RestChannel> received = new AtomicReference<>();
    Consumer<RestChannel> work =
        ch -> {
          ran.set(true);
          received.set(ch);
        };

    SqlCoordinatorTaskDispatcher.PASSTHROUGH.dispatch(client, "SELECT 1", channel, work);

    assertTrue(ran.get());
    assertSame(channel, received.get());
  }
}
