/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Verifies that outputlookup writes run under the calling user's permissions (not a privileged
 * plugin context). A read-only user can read the source but has no create, alias, or write
 * permission on the destination, so the command must be denied. If the write silently ran as a
 * system identity, this query would succeed, which would be a privilege escalation.
 */
public class OutputLookupPermissionsIT extends SecurityTestBase {

  private static final String SRC = "olk_perm_src";
  private static final String DEST = "olk_perm_dest";
  private static final String RO_ROLE = "olk_ro_role";
  private static final String RO_USER = "olk_ro_user";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    super.init();
    if (!initialized) {
      Request create = new Request("PUT", "/" + SRC);
      create.setJsonEntity(
          "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"}}}}");
      client().performRequest(create);
      Request doc = new Request("PUT", "/" + SRC + "/_doc/1?refresh=true");
      doc.setJsonEntity("{\"id\":1}");
      client().performRequest(doc);

      // Read-only role over the source: search + PIT + ppl, but no write/create/alias.
      createRoleWithIndexAccess(RO_ROLE, SRC);
      createUser(RO_USER, RO_ROLE);
      initialized = true;
    }
    enableCalcite();
    allowCalciteFallback();
  }

  @Test
  public void writeDeniedForReadOnlyUser() {
    String query = String.format(Locale.ROOT, "source=%s | fields id | outputlookup %s", SRC, DEST);
    ResponseException ex =
        assertThrows(ResponseException.class, () -> executeQueryAsUser(query, RO_USER));
    String body = ex.getMessage();
    assertTrue(
        "denial should be a security/permissions error, got: " + body,
        body.contains("security_exception")
            || body.contains("no permissions")
            || body.contains("unauthorized"));
  }
}
