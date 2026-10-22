/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;

class OpenSearchSecurityAdapterTest {

  @Test
  void current_returnsUnsecuredWhenNoSecurityContext() {
    ThreadContext ctx = new ThreadContext(Settings.EMPTY);
    OpenSearchSecurityAdapter adapter = new OpenSearchSecurityAdapter(ctx);
    assertSame(Principal.UNSECURED, adapter.current());
  }

  @Test
  void current_capturesUserFromTransientKey() {
    User user = mock(User.class);
    when(user.getName()).thenReturn("alice");
    when(user.getRequestedTenant()).thenReturn("tenant-a");
    when(user.getBackendRoles()).thenReturn(List.of("readers"));

    ThreadContext ctx = new ThreadContext(Settings.EMPTY);
    ctx.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, user);
    OpenSearchSecurityAdapter adapter = new OpenSearchSecurityAdapter(ctx);
    Principal principal = adapter.current();
    assertEquals("alice", principal.name());
    assertEquals("tenant-a", principal.tenant());
    assertEquals(List.of("readers"), principal.backendRoles());
  }

  @Test
  void current_forbidsOnUnparseableTransient() {
    ThreadContext ctx = new ThreadContext(Settings.EMPTY);
    ctx.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, new Object());
    OpenSearchSecurityAdapter adapter = new OpenSearchSecurityAdapter(ctx);
    assertThrows(OpenSearchSecurityException.class, adapter::current);
  }
}
