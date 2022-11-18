/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.catalog.model.auth;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.model.auth.AuthenticationType;

class AuthenticationTypeTest {
  @Test
  void getAuthType() {
    assertEquals(
        AuthenticationType.BASICAUTH,
        AuthenticationType.get(AuthenticationType.BASICAUTH.getName()));
    assertEquals(
        AuthenticationType.AWSSIGV4AUTH,
        AuthenticationType.get(AuthenticationType.AWSSIGV4AUTH.getName()));
  }

  @Test
  void getNotExistAuthType() {
    assertNull(AuthenticationType.get("mock"));
  }
}
