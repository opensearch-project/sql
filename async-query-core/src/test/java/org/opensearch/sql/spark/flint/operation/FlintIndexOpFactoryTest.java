/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.flint.FlintIndexClient;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

@ExtendWith(MockitoExtension.class)
class FlintIndexOpFactoryTest {
  public static final String DATASOURCE_NAME = "DATASOURCE_NAME";

  @Mock private FlintIndexStateModelService flintIndexStateModelService;
  @Mock private FlintIndexClient flintIndexClient;
  @Mock private FlintIndexMetadataService flintIndexMetadataService;
  @Mock private EMRServerlessClientFactory emrServerlessClientFactory;

  @InjectMocks FlintIndexOpFactory flintIndexOpFactory;

  @Test
  void getDrop() {
    assertNotNull(flintIndexOpFactory.getDrop(DATASOURCE_NAME));
  }

  @Test
  void getAlter() {
    assertNotNull(flintIndexOpFactory.getAlter(new FlintIndexOptions(), DATASOURCE_NAME));
  }

  @Test
  void getVacuum() {
    assertNotNull(flintIndexOpFactory.getDrop(DATASOURCE_NAME));
  }

  @Test
  void getCancel() {
    assertNotNull(flintIndexOpFactory.getDrop(DATASOURCE_NAME));
  }
}
