/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.storage;

import static org.opensearch.sql.datasources.storage.OpenSearchDataSourceMetadataStorage.DATASOURCE_INDEX_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.encryptor.Encryptor;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;

@ExtendWith(MockitoExtension.class)
public class OpenSearchDataSourceMetadataStorageTest {

  private static final String TEST_DATASOURCE_INDEX_NAME = "testDS";

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;
  @Mock
  private Encryptor encryptor;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private SearchResponse searchResponse;
  @Mock
  private ActionFuture<SearchResponse> searchResponseActionFuture;
  @Mock
  private ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
  @Mock
  private ActionFuture<IndexResponse> indexResponseActionFuture;
  @Mock
  private IndexResponse indexResponse;
  @Mock
  private ActionFuture<UpdateResponse> updateResponseActionFuture;
  @Mock
  private UpdateResponse updateResponse;
  @Mock
  private ActionFuture<DeleteResponse> deleteResponseActionFuture;
  @Mock
  private DeleteResponse deleteResponse;
  @Mock
  private SearchHit searchHit;
  @InjectMocks
  private OpenSearchDataSourceMetadataStorage openSearchDataSourceMetadataStorage;


  @SneakyThrows
  @Test
  public void testGetDataSourceMetadata() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    Mockito.when(searchHit.getSourceAsString())
        .thenReturn(getBasicDataSourceMetadataString());
    Mockito.when(encryptor.decrypt("password")).thenReturn("password");
    Mockito.when(encryptor.decrypt("username")).thenReturn("username");

    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);


    Assertions.assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    Assertions.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    Assertions.assertEquals("password",
        dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    Assertions.assertEquals("username",
        dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    Assertions.assertEquals("basicauth",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
  }

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWith404SearchResponse() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.NOT_FOUND);

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> openSearchDataSourceMetadataStorage.getDataSourceMetadata(
            TEST_DATASOURCE_INDEX_NAME));
    Assertions.assertEquals(
        "Fetching dataSource metadata information failed with status : NOT_FOUND",
        runtimeException.getMessage());
  }

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWithParsingFailed() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    Mockito.when(searchHit.getSourceAsString())
        .thenReturn("..testDs");

    Assertions.assertThrows(RuntimeException.class,
        () -> openSearchDataSourceMetadataStorage.getDataSourceMetadata(
            TEST_DATASOURCE_INDEX_NAME));
  }

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWithAWSSigV4() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    Mockito.when(searchHit.getSourceAsString())
        .thenReturn(getAWSSigv4DataSourceMetadataString());
    Mockito.when(encryptor.decrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.decrypt("access_key")).thenReturn("access_key");

    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);


    Assertions.assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    Assertions.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    Assertions.assertEquals("secret_key",
        dataSourceMetadata.getProperties().get("prometheus.auth.secret_key"));
    Assertions.assertEquals("access_key",
        dataSourceMetadata.getProperties().get("prometheus.auth.access_key"));
    Assertions.assertEquals("awssigv4",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
  }

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWithBasicAuth() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    Mockito.when(searchHit.getSourceAsString())
        .thenReturn(getDataSourceMetadataStringWithBasicAuthentication());
    Mockito.when(encryptor.decrypt("username")).thenReturn("username");
    Mockito.when(encryptor.decrypt("password")).thenReturn("password");

    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);


    Assertions.assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    Assertions.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    Assertions.assertEquals("username",
        dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    Assertions.assertEquals("password",
        dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    Assertions.assertEquals("basicauth",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
  }


  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataList() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    Mockito.when(searchHit.getSourceAsString())
        .thenReturn(getDataSourceMetadataStringWithNoAuthentication());

    List<DataSourceMetadata> dataSourceMetadataList
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata();


    Assertions.assertEquals(1, dataSourceMetadataList.size());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataList.get(0);
    Assertions.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
  }


  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataListWithNoIndex() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);

    List<DataSourceMetadata> dataSourceMetadataList
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata();

    Assertions.assertEquals(0, dataSourceMetadataList.size());
  }

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWithNoIndex() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);

    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);

    Assertions.assertFalse(dataSourceMetadataOptional.isPresent());
  }

  @Test
  public void testCreateDataSourceMetadata() {

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(dataSourceMetadata);

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();


  }

  @Test
  public void testCreateDataSourceMetadataWithOutCreatingIndex() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.TRUE);
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(dataSourceMetadata);

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }


  @Test
  public void testCreateDataSourceMetadataFailedWithNotFoundResponse() {

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(
            dataSourceMetadata));
    Assertions.assertEquals("Saving dataSource metadata information failed with result : not_found",
        runtimeException.getMessage());

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();


  }

  @Test
  public void testCreateDataSourceMetadataWithVersionConflict() {

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any()))
        .thenThrow(VersionConflictEngineException.class);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(
                dataSourceMetadata));
    Assertions.assertEquals("A datasource already exists with name: testDS",
        illegalArgumentException.getMessage());


    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();


  }

  @Test
  public void testCreateDataSourceMetadataWithException() {

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any()))
        .thenThrow(new RuntimeException("error while indexing"));
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(
            dataSourceMetadata));
    Assertions.assertEquals("java.lang.RuntimeException: error while indexing",
        runtimeException.getMessage());

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();


  }

  @Test
  public void testCreateDataSourceMetadataWithIndexCreationFailed() {

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(false, false, DATASOURCE_INDEX_NAME));
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(
            dataSourceMetadata));
    Assertions.assertEquals(
        "Internal server error while creating.ql-datasources index:: "
            + "Index creation is not acknowledged.",
        runtimeException.getMessage());

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testUpdateDataSourceMetadata() {
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.update(ArgumentMatchers.any())).thenReturn(updateResponseActionFuture);
    Mockito.when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    Mockito.when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.updateDataSourceMetadata(dataSourceMetadata);

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).update(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();

  }

  @Test
  public void testUpdateDataSourceMetadataWithNotFoundResult() {
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.update(ArgumentMatchers.any())).thenReturn(updateResponseActionFuture);
    Mockito.when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    Mockito.when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> this.openSearchDataSourceMetadataStorage.updateDataSourceMetadata(
            dataSourceMetadata));
    Assertions.assertEquals("Saving dataSource metadata information failed with result : not_found",
        runtimeException.getMessage());

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).update(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();

  }

  @Test
  public void testUpdateDataSourceMetadataWithDocumentMissingException() {
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.update(ArgumentMatchers.any())).thenThrow(new DocumentMissingException(
        ShardId.fromString("[2][2]"), "testDS"));
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();
    dataSourceMetadata.setName("testDS");


    DataSourceNotFoundException dataSourceNotFoundException =
        Assertions.assertThrows(DataSourceNotFoundException.class,
            () -> this.openSearchDataSourceMetadataStorage.updateDataSourceMetadata(
                dataSourceMetadata));
    Assertions.assertEquals("Datasource with name: testDS doesn't exist",
        dataSourceNotFoundException.getMessage());

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).update(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();

  }

  @Test
  public void testUpdateDataSourceMetadataWithRuntimeException() {
    Mockito.when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    Mockito.when(encryptor.encrypt("access_key")).thenReturn("access_key");
    Mockito.when(client.update(ArgumentMatchers.any()))
        .thenThrow(new RuntimeException("error message"));
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();
    dataSourceMetadata.setName("testDS");


    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> this.openSearchDataSourceMetadataStorage.updateDataSourceMetadata(
            dataSourceMetadata));
    Assertions.assertEquals("java.lang.RuntimeException: error message",
        runtimeException.getMessage());

    Mockito.verify(encryptor, Mockito.times(1)).encrypt("secret_key");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("access_key");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).update(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();

  }

  @Test
  public void testDeleteDataSourceMetadata() {
    Mockito.when(client.delete(ArgumentMatchers.any())).thenReturn(deleteResponseActionFuture);
    Mockito.when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    Mockito.when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);

    this.openSearchDataSourceMetadataStorage.deleteDataSourceMetadata("testDS");

    Mockito.verifyNoInteractions(encryptor);
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).delete(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testDeleteDataSourceMetadataWhichisAlreadyDeleted() {
    Mockito.when(client.delete(ArgumentMatchers.any())).thenReturn(deleteResponseActionFuture);
    Mockito.when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    Mockito.when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);

    DataSourceNotFoundException dataSourceNotFoundException =
        Assertions.assertThrows(DataSourceNotFoundException.class,
            () -> this.openSearchDataSourceMetadataStorage.deleteDataSourceMetadata("testDS"));
    Assertions.assertEquals("Datasource with name: testDS doesn't exist",
        dataSourceNotFoundException.getMessage());


    Mockito.verifyNoInteractions(encryptor);
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).delete(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testDeleteDataSourceMetadataWithUnexpectedResult() {
    Mockito.when(client.delete(ArgumentMatchers.any())).thenReturn(deleteResponseActionFuture);
    Mockito.when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    Mockito.when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.NOOP);

    RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class,
        () -> this.openSearchDataSourceMetadataStorage.deleteDataSourceMetadata("testDS"));
    Assertions.assertEquals("Deleting dataSource metadata information failed with result : noop",
        runtimeException.getMessage());

    Mockito.verifyNoInteractions(encryptor);
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).delete(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  private String getBasicDataSourceMetadataString() throws JsonProcessingException {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(Collections.singletonList("prometheus_access"));
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "username");
    properties.put("prometheus.auth.uri", "https://localhost:9090");
    properties.put("prometheus.auth.password", "password");
    dataSourceMetadata.setProperties(properties);
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(dataSourceMetadata);
  }

  private String getAWSSigv4DataSourceMetadataString() throws JsonProcessingException {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(Collections.singletonList("prometheus_access"));
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.secret_key", "secret_key");
    properties.put("prometheus.auth.uri", "https://localhost:9090");
    properties.put("prometheus.auth.access_key", "access_key");
    dataSourceMetadata.setProperties(properties);
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(dataSourceMetadata);
  }

  private String getDataSourceMetadataStringWithBasicAuthentication()
      throws JsonProcessingException {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(Collections.singletonList("prometheus_access"));
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.uri", "https://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "username");
    properties.put("prometheus.auth.password", "password");
    dataSourceMetadata.setProperties(properties);
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(dataSourceMetadata);
  }

  private String getDataSourceMetadataStringWithNoAuthentication() throws JsonProcessingException {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(Collections.singletonList("prometheus_access"));
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.uri", "https://localhost:9090");
    dataSourceMetadata.setProperties(properties);
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(dataSourceMetadata);
  }

  private DataSourceMetadata getDataSourceMetadata() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(Collections.singletonList("prometheus_access"));
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.secret_key", "secret_key");
    properties.put("prometheus.auth.uri", "https://localhost:9090");
    properties.put("prometheus.auth.access_key", "access_key");
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

}
