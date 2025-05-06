/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

@ExtendWith(MockitoExtension.class)
class SparkParameterComposerCollectionTest {

  @Mock DataSourceSparkParameterComposer composer1;
  @Mock DataSourceSparkParameterComposer composer2;
  @Mock DataSourceSparkParameterComposer composer3;
  @Mock GeneralSparkParameterComposer generalComposer;
  @Mock DispatchQueryRequest dispatchQueryRequest;
  @Mock AsyncQueryRequestContext asyncQueryRequestContext;

  final DataSourceType type1 = new DataSourceType("TYPE1");
  final DataSourceType type2 = new DataSourceType("TYPE2");
  final DataSourceType type3 = new DataSourceType("TYPE3");

  SparkParameterComposerCollection collection;

  @BeforeEach
  void setUp() {
    collection = new SparkParameterComposerCollection();
    collection.register(type1, composer1);
    collection.register(type1, composer2);
    collection.register(type2, composer3);
    collection.register(generalComposer);
  }

  @Test
  void isComposerRegistered() {
    assertTrue(collection.isComposerRegistered(type1));
    assertTrue(collection.isComposerRegistered(type2));
    assertFalse(collection.isComposerRegistered(type3));
  }

  @Test
  void composeByDataSourceWithRegisteredType() {
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder().setConnector(type1).setName("name").build();
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    collection.composeByDataSource(
        metadata, sparkSubmitParameters, dispatchQueryRequest, asyncQueryRequestContext);

    verify(composer1)
        .compose(metadata, sparkSubmitParameters, dispatchQueryRequest, asyncQueryRequestContext);
    verify(composer2)
        .compose(metadata, sparkSubmitParameters, dispatchQueryRequest, asyncQueryRequestContext);
    verifyNoInteractions(composer3);
  }

  @Test
  void composeByDataSourceWithUnregisteredType() {
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder().setConnector(type3).setName("name").build();
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    collection.composeByDataSource(
        metadata, sparkSubmitParameters, dispatchQueryRequest, asyncQueryRequestContext);

    verifyNoInteractions(composer1, composer2, composer3);
  }

  @Test
  void compose() {
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    collection.compose(sparkSubmitParameters, dispatchQueryRequest, asyncQueryRequestContext);

    verify(generalComposer)
        .compose(sparkSubmitParameters, dispatchQueryRequest, asyncQueryRequestContext);
    verifyNoInteractions(composer1, composer2, composer3);
  }
}
