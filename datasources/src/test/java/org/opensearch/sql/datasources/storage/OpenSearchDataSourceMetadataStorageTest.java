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
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.datasources.encryptor.Encryptor;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

@RunWith(MockitoJUnitRunner.class)
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


    Assert.assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    Assert.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assert.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    Assert.assertEquals("password",
        dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    Assert.assertEquals("username",
        dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    Assert.assertEquals("basicauth",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
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


    Assert.assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    Assert.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assert.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    Assert.assertEquals("secret_key",
        dataSourceMetadata.getProperties().get("prometheus.auth.secret_key"));
    Assert.assertEquals("access_key",
        dataSourceMetadata.getProperties().get("prometheus.auth.access_key"));
    Assert.assertEquals("awssigv4",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
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
  public void testUpdateDataSourceMetadata() {
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.TRUE);
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
  public void testDeleteDataSourceMetadata() {
    Mockito.when(client.delete(ArgumentMatchers.any())).thenReturn(deleteResponseActionFuture);
    Mockito.when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    Mockito.when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.deleteDataSourceMetadata("testDS");

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
