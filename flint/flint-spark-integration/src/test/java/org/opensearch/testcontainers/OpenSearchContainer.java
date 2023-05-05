/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * OpenSearchContainer Implementation.
 */
public class OpenSearchContainer extends GenericContainer<OpenSearchContainer> {

  private static final int DEFAULT_PORT = 9200;

  private static final String DEFAULT_IMAGE_NAME = "opensearchproject/opensearch:2.6.0";

  public OpenSearchContainer() {
    this(DockerImageName.parse(DEFAULT_IMAGE_NAME));
  }

  public OpenSearchContainer(String dockerImageName) {
    this(DockerImageName.parse(dockerImageName));
  }

  /**
   * Default OpenSearch Container.
   * Single node.
   * Disable security,
   */
  public OpenSearchContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);

    withExposedPorts(DEFAULT_PORT);
    withEnv("discovery.type", "single-node");
    withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true");
    withEnv("DISABLE_SECURITY_PLUGIN", "true");
  }

  public int port() {
    return getMappedPort(DEFAULT_PORT);
  }
}
