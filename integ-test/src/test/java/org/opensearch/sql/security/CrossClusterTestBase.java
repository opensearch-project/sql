/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATA;

import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CrossClusterTestBase extends PPLIntegTestCase {
  static {
    // find a remote cluster
    String[] clusterNames = System.getProperty("cluster.names").split(",");
    var remote = "remoteCluster";
    for (var cluster : clusterNames) {
      if (cluster.startsWith("remote")) {
        remote = cluster;
        break;
      }
    }
    REMOTE_CLUSTER = remote;
  }

  public static final String REMOTE_CLUSTER;

  protected static final String TEST_INDEX_BANK_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
  protected static final String TEST_INDEX_DOG_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  protected static final String TEST_INDEX_DOG_MATCH_ALL_REMOTE =
      MATCH_ALL_REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  protected static final String TEST_INDEX_ACCOUNT_REMOTE =
      REMOTE_CLUSTER + ":" + TEST_INDEX_ACCOUNT;
  protected static final String TEST_INDEX_TIME_DATA_REMOTE =
      REMOTE_CLUSTER + ":" + TEST_INDEX_TIME_DATA;

  @Override
  protected void init() throws Exception {
    super.init();
    configureMultiClusters(REMOTE_CLUSTER);
  }
}
