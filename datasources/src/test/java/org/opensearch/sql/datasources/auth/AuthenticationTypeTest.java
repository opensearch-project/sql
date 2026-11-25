/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class AuthenticationTypeTest {
  @Test
  void getAuthType() {
    assertEquals(
        AuthenticationType.BASICAUTH,
        AuthenticationType.get(AuthenticationType.BASICAUTH.getName()));
    assertEquals(
        AuthenticationType.AWSSIGV4AUTH,
        AuthenticationType.get(AuthenticationType.AWSSIGV4AUTH.getName()));
    assertEquals(
        AuthenticationType.OAUTH2, AuthenticationType.get(AuthenticationType.OAUTH2.getName()));
  }

  @Test
  void getAuthTypeByStringLiteral() {
    // Test direct string lookup (not using enum.getName())
    assertEquals(AuthenticationType.BASICAUTH, AuthenticationType.get("basicauth"));
    assertEquals(AuthenticationType.AWSSIGV4AUTH, AuthenticationType.get("awssigv4"));
    assertEquals(AuthenticationType.OAUTH2, AuthenticationType.get("oauth2"));
    assertEquals(AuthenticationType.NOAUTH, AuthenticationType.get("noauth"));
  }

  @Test
  void getNameReturnsCorrectString() {
    // Test that getName() returns the expected string values
    assertEquals("basicauth", AuthenticationType.BASICAUTH.getName());
    assertEquals("awssigv4", AuthenticationType.AWSSIGV4AUTH.getName());
    assertEquals("oauth2", AuthenticationType.OAUTH2.getName());
    assertEquals("noauth", AuthenticationType.NOAUTH.getName());
  }

  @Test
  void getNotExistAuthType() {
    assertNull(AuthenticationType.get("mock"));
  }
}
