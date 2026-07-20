/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;

/**
 * Verifies that outputlookup writes run under the calling user's permissions (not a privileged
 * plugin context). Under substrate C3 the write creates or writes the shared {@code .lookups} index
 * and adds or repoints the {@code <name>} filtered alias; a read-only user can read the source but
 * has no create, write, or alias permission for that, so the command must be denied. If the write
 * silently ran as a system identity, this query would succeed, which would be a privilege
 * escalation.
 */
public class OutputLookupPermissionsIT extends SecurityTestBase {

  private static final String SRC = "olk_perm_src";
  private static final String DEST = "olk_perm_dest";
  private static final String RO_ROLE = "olk_ro_role";
  private static final String RO_USER = "olk_ro_user";
  private static final String RW_ROLE = "olk_rw_role";
  private static final String RW_USER = "olk_rw_user";
  private static final String RW_DEST = "olk_perm_rw_lookup";

  private static boolean initialized = false;

  @Override
  protected void init() throws Exception {
    super.init();
    if (!initialized) {
      Request create = new Request("PUT", "/" + SRC);
      create.setJsonEntity("{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"}}}}");
      client().performRequest(create);
      Request doc = new Request("PUT", "/" + SRC + "/_doc/1?refresh=true");
      doc.setJsonEntity("{\"id\":1}");
      client().performRequest(doc);

      // Read-only role over the source: search + PIT + ppl, but no write/create/alias.
      createRoleWithIndexAccess(RO_ROLE, SRC);
      createUser(RO_USER, RO_ROLE);

      // Reader role: read + PIT + ppl over all indices including dot-prefixed ones (so it can read
      // the shared .lookups index a lookup resolves to). Granting ".*" explicitly is required
      // because a "*" pattern does not cover dot-prefixed indices in the security plugin; that it
      // works at all proves .lookups is an ordinary index, not a registered system index (which
      // would stay unreadable to a non-system caller even when named).
      Request rwRole = new Request("PUT", "/_plugins/_security/api/roles/" + RW_ROLE);
      rwRole.setJsonEntity(
          "{\"cluster_permissions\":[\"cluster:admin/opensearch/ppl\"],"
              + "\"index_permissions\":[{\"index_patterns\":[\"*\",\".*\"],"
              + "\"allowed_actions\":[\"indices:data/read/search*\",\"indices:admin/mappings/get\","
              + "\"indices:monitor/settings/get\",\"indices:admin/aliases/get\","
              + "\"indices:data/read/point_in_time/create\","
              + "\"indices:data/read/point_in_time/delete\"]}]}");
      RequestOptions.Builder rwOpts = RequestOptions.DEFAULT.toBuilder();
      rwOpts.addHeader("Content-Type", "application/json");
      rwRole.setOptions(rwOpts);
      client().performRequest(rwRole);
      createUser(RW_USER, RW_ROLE);
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

  @Test
  public void readLookupAsAuthorizedUser() throws IOException {
    // Admin writes a lookup: creates .lookups, writes a __lookup slice, and adds the filtered
    // alias.
    executeQuery(
        String.format(Locale.ROOT, "source=%s | fields id | outputlookup %s", SRC, RW_DEST));

    // A user with only read privileges reads the lookup back through its filtered alias. This is
    // the point of NOT registering .lookups as a system index: an ordinary read-privileged user can
    // read a lookup via source=<name>. A registered system index would block this read for a
    // non-system caller.
    JSONObject r =
        executeQueryAsUser(
            String.format(Locale.ROOT, "source=%s | stats count() as c", RW_DEST), RW_USER);
    assertEquals(
        "authorized user can read the lookup",
        1L,
        r.getJSONArray("datarows").getJSONArray(0).getLong(0));
  }
}
