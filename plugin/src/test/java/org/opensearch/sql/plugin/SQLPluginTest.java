/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.security.Principal;
import org.junit.Test;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.identity.PluginSubject;
import org.opensearch.sql.opensearch.client.PluginClient;
import org.opensearch.transport.client.Client;

/** Tests the subject wiring of {@link SQLPlugin}. */
public class SQLPluginTest {

  @Test
  public void assignSubjectGivesTheSubjectToThePluginsOwnClient() throws Exception {
    SQLPlugin plugin = new SQLPlugin();
    PluginClient pluginClient = new PluginClient(mock(Client.class));
    // Stands in for createComponents, which builds this client from the node client.
    Field field = SQLPlugin.class.getDeclaredField("pluginClient");
    field.setAccessible(true);
    field.set(plugin, pluginClient);

    FakeSubject subject = new FakeSubject();
    plugin.assignSubject(subject);

    // The client refuses a different subject once one is assigned, and accepts the same one again.
    assertThrows(IllegalStateException.class, () -> pluginClient.setSubject(new FakeSubject()));
    pluginClient.setSubject(subject);
  }

  private static class FakeSubject implements PluginSubject {

    @Override
    public Principal getPrincipal() {
      return () -> "plugin:test";
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> runnable) throws E {
      runnable.run();
    }
  }
}
