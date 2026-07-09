/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Security integration tests for the PPL {@code collect} command. collect materializes into a
 * destination index through an asynchronous background task authorized as {@code
 * cluster:admin/opensearch/ppl/collect/materialize}, so a role needs that cluster permission plus
 * write permission on the destination in addition to the base PPL read permissions. Because the
 * write is fire-and-forget, a missing materialize permission does not fail the request
 * synchronously; the foreground still returns the preview and a task id, and the destination simply
 * is never written.
 */
public class CollectPermissionsIT extends SecurityTestBase {

  private static final String SRC = "collect_perm_src";
  private static final String DEST = "collect_perm_dest";
  private static final String DEST_DENIED = "collect_perm_dest_denied";

  private static final String FULL_USER = "collect_full_user";
  private static final String FULL_ROLE = "collect_full_role";
  private static final String NO_MATERIALIZE_USER = "collect_no_materialize_user";
  private static final String NO_MATERIALIZE_ROLE = "collect_no_materialize_role";

  private static final String MAPPING =
      "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
          + "\"name\":{\"type\":\"keyword\"}}}}";

  private static final String[] COLLECT_INDEX_PERMISSIONS =
      new String[] {
        "indices:data/read/search*",
        "indices:admin/mappings/get",
        "indices:monitor/settings/get",
        "indices:data/read/point_in_time/create",
        "indices:data/read/point_in_time/delete",
        "indices:data/write/*"
      };

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    if (!initialized) {
      createIndexWithMapping(SRC, MAPPING);
      createIndexWithMapping(DEST, MAPPING);
      createIndexWithMapping(DEST_DENIED, MAPPING);
      indexSourceDocs();

      // Full role: base PPL plus the collect materialize cluster action, read on the source and
      // write on the destination (one pattern covers all collect_perm_* indices).
      createRoleWithPermissions(
          FULL_ROLE,
          "collect_perm_*",
          new String[] {
            "cluster:admin/opensearch/ppl", "cluster:admin/opensearch/ppl/collect/materialize"
          },
          COLLECT_INDEX_PERMISSIONS);
      createUser(FULL_USER, FULL_ROLE);

      // Same index permissions but WITHOUT the collect materialize cluster action.
      createRoleWithPermissions(
          NO_MATERIALIZE_ROLE,
          "collect_perm_*",
          new String[] {"cluster:admin/opensearch/ppl"},
          COLLECT_INDEX_PERMISSIONS);
      createUser(NO_MATERIALIZE_USER, NO_MATERIALIZE_ROLE);
      initialized = true;
    }
  }

  private void indexSourceDocs() throws IOException {
    Request bulk = new Request("POST", "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(
        "{\"index\":{\"_index\":\""
            + SRC
            + "\",\"_id\":1}}\n{\"id\":1,\"name\":\"a\"}\n"
            + "{\"index\":{\"_index\":\""
            + SRC
            + "\",\"_id\":2}}\n{\"id\":2,\"name\":\"b\"}\n"
            + "{\"index\":{\"_index\":\""
            + SRC
            + "\",\"_id\":3}}\n{\"id\":3,\"name\":\"c\"}\n");
    client().performRequest(bulk);
  }

  @Test
  public void testCollectWithMaterializePermissionWritesDestination() throws IOException {
    JSONObject result =
        executeQueryAsUser(
            String.format("source=%s | fields id | collect index=%s", SRC, DEST), FULL_USER);
    assertTrue("async collect must return a task_id", result.has("task_id"));
    // The background materialization runs under the user's materialize plus write permissions.
    awaitCount(DEST, 3L);
  }

  @Test
  public void testCollectWithoutMaterializePermissionDoesNotWrite() throws IOException {
    // Fire-and-forget: the foreground still returns preview rows and a task id even though the
    // background materialize action is authorization denied for this role.
    JSONObject result =
        executeQueryAsUser(
            String.format("source=%s | fields id | collect index=%s", SRC, DEST_DENIED),
            NO_MATERIALIZE_USER);
    assertTrue(result.has("task_id"));
    // The destination is never written because cluster:admin/opensearch/ppl/collect/materialize is
    // not granted to this role.
    assertStaysEmpty(DEST_DENIED);
  }

  private long adminCount(String index) throws IOException {
    client().performRequest(new Request("POST", "/" + index + "/_refresh"));
    Response response = client().performRequest(new Request("GET", "/" + index + "/_count"));
    return new JSONObject(TestUtils.getResponseBody(response, true)).getLong("count");
  }

  private void awaitCount(String index, long expected) throws IOException {
    long deadline = System.currentTimeMillis() + 30_000L;
    long actual = -1L;
    while (System.currentTimeMillis() < deadline) {
      actual = adminCount(index);
      if (actual == expected) {
        return;
      }
      sleep(500L);
    }
    assertEquals(
        "collect must eventually make all rows visible in the destination", expected, actual);
  }

  private void assertStaysEmpty(String index) throws IOException {
    long deadline = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < deadline) {
      assertEquals(
          "destination must stay empty when the collect materialize permission is missing",
          0L,
          adminCount(index));
      sleep(1_000L);
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
