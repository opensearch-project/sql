/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
    ThreadContext context = new ThreadContext(Settings.EMPTY);
    context.putTransient(
        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
        "alice|backend-a|ppl-role|tenant-a");

    PPLAsyncQueryUser identity = PPLAsyncQueryUser.current(context);

    assertEquals("alice", identity.name());
    assertEquals("tenant-a", identity.requestedTenant());
    assertEquals(List.of("backend-a"), identity.backendRoles());
  }

  @Test
  public void acceptsUserObjectAndRejectsUnknownSecurityContext() {
    ThreadContext objectContext = new ThreadContext(Settings.EMPTY);
    objectContext.putTransient(
        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
        new User("alice", List.of("backend-a"), List.of("ppl-role"), null, "tenant-a"));
    assertEquals("alice", PPLAsyncQueryUser.current(objectContext).name());

    ThreadContext invalidContext = new ThreadContext(Settings.EMPTY);
    invalidContext.putTransient(
        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, new Object());
    assertThrows(
        OpenSearchSecurityException.class, () -> PPLAsyncQueryUser.current(invalidContext));
  }

  @Test
  public void missingIdentityRepresentsAnUnsecuredCaller() {
    ThreadContext context = new ThreadContext(Settings.EMPTY);

    assertNull(PPLAsyncQueryUser.current(context).name());
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
    PPLAsyncQueryUser unsecured = new PPLAsyncQueryUser(null, null, List.of());
    unsecured.authorize(new PPLAsyncQueryUser(null, null, List.of()));

    assertThrows(
        OpenSearchSecurityException.class,
        () -> unsecured.authorize(new PPLAsyncQueryUser("alice", null, List.of("role-a"))));
  }
}
