/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.identity.Subject;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.FilterClient;

/**
 * Executes transport actions as this plugin's assigned system subject rather than as the
 * authenticated user, which is how the plugin reaches the system indices it owns.
 */
public class PluginClient extends FilterClient {

  private static final Logger LOG = LogManager.getLogger(PluginClient.class);

  // Assigned from IdentityAwarePlugin.assignSubject, which runs on a different thread than the
  // transport actions that read it.
  private volatile Subject subject;

  public PluginClient(Client delegate) {
    super(delegate);
  }

  public void setSubject(Subject subject) {
    this.subject = subject;
  }

  @Override
  protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
      ActionType<Response> action, Request request, ActionListener<Response> listener) {
    Subject currentSubject = this.subject;
    if (currentSubject == null) {
      throw new IllegalStateException("PluginClient is not initialized with a subject.");
    }

    // Saves the caller's context so the listener can be given it back. runAs performs the switch
    // itself and restores on exit, so this is about the listener, which runs later and on a thread
    // that never carried the caller's context.
    ThreadContext.StoredContext storedContext =
        threadPool().getThreadContext().newStoredContext(false);

    try {
      currentSubject.runAs(
          () -> {
            LOG.debug(
                "Running transport action as subject: {}", currentSubject.getPrincipal().getName());
            super.doExecute(
                action, request, ActionListener.runBefore(listener, storedContext::restore));
          });
    } catch (Exception e) {
      // Reported through the listener rather than thrown, so an async caller is not left waiting.
      storedContext.close();
      listener.onFailure(e);
    }
  }
}
