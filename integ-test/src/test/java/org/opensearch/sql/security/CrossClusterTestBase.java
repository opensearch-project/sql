/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MVEXPAND_EDGE_CASES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATA;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CrossClusterTestBase extends PPLIntegTestCase {
  static {
    // find a remote cluster; the "cluster.names" property is only set by tasks that stand up a
    // second (remote) cluster, so it may be absent on a plain single-cluster integTestRemote run.
    String clusterNamesProp = System.getProperty("cluster.names");
    String remote = null;
    if (clusterNamesProp != null && !clusterNamesProp.isBlank()) {
      for (var cluster : clusterNamesProp.split(",")) {
        if (cluster.startsWith("remote")) {
          remote = cluster;
          break;
        }
      }
    }
    HAS_REMOTE_CLUSTER = remote != null;
    REMOTE_CLUSTER = remote != null ? remote : "remoteCluster";
  }

  /** True only when a remote cluster is configured; cross-cluster tests are skipped otherwise. */
  public static final boolean HAS_REMOTE_CLUSTER;

  public static final String REMOTE_CLUSTER;

  @BeforeClass
  public static void requireRemoteCluster() {
    Assume.assumeTrue(
        "Cross-cluster search requires a configured remote cluster (-Dcluster.names must include a"
            + " 'remote*' cluster); skipping",
        HAS_REMOTE_CLUSTER);
  }

  protected static final String TEST_INDEX_BANK_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
  protected static final String TEST_INDEX_DOG_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  protected static final String TEST_INDEX_DOG_MATCH_ALL_REMOTE =
      MATCH_ALL_REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  protected static final String TEST_INDEX_ACCOUNT_REMOTE =
      REMOTE_CLUSTER + ":" + TEST_INDEX_ACCOUNT;
  protected static final String TEST_INDEX_TIME_DATA_REMOTE =
      REMOTE_CLUSTER + ":" + TEST_INDEX_TIME_DATA;
  protected static final String TEST_INDEX_MVEXPAND_REMOTE =
      REMOTE_CLUSTER + ":" + TEST_INDEX_MVEXPAND_EDGE_CASES;

  @Override
  protected void init() throws Exception {
    super.init();
    configureMultiClusters(REMOTE_CLUSTER);
  }
}
