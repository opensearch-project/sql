/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;

public class PPLAsyncQueryUserTest {

  @Test
  public void capturesSecurityIdentity() {
    ThreadContext context = contextWith("alice|backend-a|ppl-role|tenant-a");

    assertEquals(
        new PPLAsyncQueryUser("alice", "tenant-a", List.of("backend-a")),
        PPLAsyncQueryUser.current(context));
  }

  @Test
  public void acceptsUserObject() {
    ThreadContext context =
        contextWith(new User("alice", List.of("backend-a"), List.of("ppl-role"), null, "tenant-a"));

    assertEquals(
        new PPLAsyncQueryUser("alice", "tenant-a", List.of("backend-a")),
        PPLAsyncQueryUser.current(context));
  }

  @Test
  public void rejectsUnknownSecurityContext() {
    assertThrows(
        OpenSearchSecurityException.class,
        () -> PPLAsyncQueryUser.current(contextWith(new Object())));
  }

  @Test
  public void missingIdentityRepresentsAnUnsecuredCaller() {
    ThreadContext context = new ThreadContext(Settings.EMPTY);

    assertEquals(PPLAsyncQueryUser.UNSECURED, PPLAsyncQueryUser.current(context));
  }

  @Test
  public void requiresSamePrincipalTenantAndOriginalBackendRoles() {
    PPLAsyncQueryUser owner = new PPLAsyncQueryUser("alice", "tenant-a", List.of("role-a"));

    owner.authorize(
        new PPLAsyncQueryUser("alice", "tenant-a", List.of("role-a", "newly-added-role")));

    assertThrows(
        OpenSearchSecurityException.class,
        () -> owner.authorize(new PPLAsyncQueryUser("bob", "tenant-a", List.of("role-a"))));
    assertThrows(
        OpenSearchSecurityException.class,
        () -> owner.authorize(new PPLAsyncQueryUser("alice", "tenant-b", List.of("role-a"))));
    assertThrows(
        OpenSearchSecurityException.class,
        () -> owner.authorize(new PPLAsyncQueryUser("alice", "tenant-a", List.of())));
  }

  @Test
  public void unsecuredModeRequiresAnUnsecuredCaller() {
    PPLAsyncQueryUser.UNSECURED.authorize(PPLAsyncQueryUser.UNSECURED);

    assertThrows(
        OpenSearchSecurityException.class,
        () ->
            PPLAsyncQueryUser.UNSECURED.authorize(
                new PPLAsyncQueryUser("alice", null, List.of("role-a"))));
  }

  private static ThreadContext contextWith(Object userInfo) {
    ThreadContext context = new ThreadContext(Settings.EMPTY);
    context.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userInfo);
    return context;
  }
}
