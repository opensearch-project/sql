/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.multicluster;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Superclass for tests that interact with multiple external test clusters using OpenSearch's {@link RestClient}.
 */
public abstract class OpenSearchMultiClustersRestTestCase extends OpenSearchRestTestCase {

  public static final String REMOTE_CLUSTER = "remoteCluster";

  private static RestClient remoteClient;
  /**
   * A client for the running remote OpenSearch cluster configured to take test administrative actions
   * like remove all indexes after the test completes
   */
  private static RestClient remoteAdminClient;

  // Modified from initClient in OpenSearchRestTestCase
  public void initRemoteClient() throws IOException {
    if (remoteClient == null) {
      assert remoteAdminClient == null;
      String cluster = getTestRestCluster(REMOTE_CLUSTER);
      String[] stringUrls = cluster.split(",");
      List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
      for (String stringUrl : stringUrls) {
        int portSeparator = stringUrl.lastIndexOf(':');
        if (portSeparator < 0) {
          throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
        }
        String host = stringUrl.substring(0, portSeparator);
        int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
        hosts.add(buildHttpHost(host, port));
      }
      final List<HttpHost> clusterHosts = unmodifiableList(hosts);
      logger.info("initializing REST clients against {}", clusterHosts);
      remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
      remoteAdminClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[0]));
    }
    assert remoteClient != null;
    assert remoteAdminClient != null;
  }

  /**
   * Get a comma delimited list of [host:port] to which to send REST requests.
   */
  protected String getTestRestCluster(String clusterName) {
    String cluster = System.getProperty("tests.rest." + clusterName + ".http_hosts");
    if (cluster == null) {
      throw new RuntimeException(
          "Must specify [tests.rest."
              + clusterName
              + ".http_hosts] system property with a comma delimited list of [host:port] "
              + "to which to send REST requests"
      );
    }
    return cluster;
  }

  /**
   * Get a comma delimited list of [host:port] for connections between clusters.
   */
  protected String getTestTransportCluster(String clusterName) {
    String cluster = System.getProperty("tests.rest." + clusterName + ".transport_hosts");
    if (cluster == null) {
      throw new RuntimeException(
          "Must specify [tests.rest."
              + clusterName
              + ".transport_hosts] system property with a comma delimited list of [host:port] "
              + "for connections between clusters"
      );
    }
    return cluster;
  }

  /**
   * Initialize rest client to remote cluster,
   * and create a connection to it from the coordinating cluster.
   */
  public void configureMultiClusters() throws IOException {
    initRemoteClient();

    Request connectionRequest = new Request("PUT", "_cluster/settings");
    String connectionSetting = "{\"persistent\": {\"cluster\": {\"remote\": {\""
        + REMOTE_CLUSTER
        + "\": {\"seeds\": [\""
        + getTestTransportCluster(REMOTE_CLUSTER).split(",")[0]
        + "\"]}}}}}";
    connectionRequest.setJsonEntity(connectionSetting);
    logger.info("Creating connection from coordinating cluster to {}", REMOTE_CLUSTER);
    adminClient().performRequest(connectionRequest);
  }

  @AfterClass
  public static void closeRemoteClients() throws IOException {
    try {
      IOUtils.close(remoteClient, remoteAdminClient);
    } finally {
      remoteClient = null;
      remoteAdminClient = null;
    }
  }

  /**
   * Get the client to remote cluster used for ordinary api calls while writing a test.
   */
  protected static RestClient remoteClient() {
    return remoteClient;
  }

  /**
   * Get the client to remote cluster used for test administrative actions.
   * Do not use this while writing a test. Only use it for cleaning up after tests.
   */
  protected static RestClient remoteAdminClient() {
    return remoteAdminClient;
  }

}
