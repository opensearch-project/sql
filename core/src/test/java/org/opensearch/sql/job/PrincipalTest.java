/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class PrincipalTest {

  @Test
  void unsecured_isOnlyAllowedByItself() {
    assertTrue(Principal.UNSECURED.allows(Principal.UNSECURED));
    assertFalse(Principal.UNSECURED.allows(new Principal("alice", null, List.of())));
    assertFalse(new Principal("alice", null, List.of()).allows(Principal.UNSECURED));
  }

  @Test
  void allows_requiresMatchingNameAndTenant() {
    Principal owner = new Principal("alice", "tenant-a", List.of("readers"));
    assertTrue(owner.allows(new Principal("alice", "tenant-a", List.of("readers", "writers"))));
    assertFalse(owner.allows(new Principal("bob", "tenant-a", List.of("readers"))));
    assertFalse(owner.allows(new Principal("alice", "tenant-b", List.of("readers"))));
  }

  @Test
  void allows_requiresCallerToCarryAllOwnerRoles() {
    Principal owner = new Principal("alice", null, List.of("readers", "auditors"));
    assertFalse(owner.allows(new Principal("alice", null, List.of("readers"))));
    assertTrue(owner.allows(new Principal("alice", null, List.of("readers", "auditors"))));
  }

  @Test
  void backendRoles_areDefensivelyCopied() {
    java.util.ArrayList<String> mutable = new java.util.ArrayList<>();
    mutable.add("readers");
    Principal principal = new Principal("alice", null, mutable);
    mutable.add("mutated");
    assertEquals(List.of("readers"), principal.backendRoles());
  }

  @Test
  void constructor_rejectsBlankName() {
    assertThrows(IllegalArgumentException.class, () -> new Principal(" ", null, List.of()));
  }

  @Test
  void constructor_acceptsNullBackendRoles() {
    Principal principal = new Principal("alice", null, null);
    assertEquals(List.of(), principal.backendRoles());
  }
}
