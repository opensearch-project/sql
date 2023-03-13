/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.plugin.datasource.OpenSearchDataSourceMetadataStorage.DATASOURCE_INDEX_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
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
import org.opensearch.sql.common.encryptor.Encryptor;
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
    when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.OK);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    when(searchHit.getSourceAsString())
        .thenReturn(getBasicDataSourceMetadataString());
    when(encryptor.decrypt("password")).thenReturn("password");
    when(encryptor.decrypt("username")).thenReturn("username");

    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);


    assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    assertEquals("password",
        dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    assertEquals("username",
        dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    assertEquals("basicauth",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
  }

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWithAWSSigV4() {
    when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.OK);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(21, TotalHits.Relation.EQUAL_TO),
                1.0F));
    when(searchHit.getSourceAsString())
        .thenReturn(getAWSSigv4DataSourceMetadataString());
    when(encryptor.decrypt("secret_key")).thenReturn("secret_key");
    when(encryptor.decrypt("access_key")).thenReturn("access_key");

    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);


    assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    assertEquals("secret_key",
        dataSourceMetadata.getProperties().get("prometheus.auth.secret_key"));
    assertEquals("access_key",
        dataSourceMetadata.getProperties().get("prometheus.auth.access_key"));
    assertEquals("awssigv4",
        dataSourceMetadata.getProperties().get("prometheus.auth.type"));
  }

  @Test
  public void testCreateDataSourceMetadata() {

    when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    when(encryptor.encrypt("access_key")).thenReturn("access_key");
    when(client.admin().indices().create(any()))
        .thenReturn(createIndexResponseActionFuture);
    when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    when(client.index(any())).thenReturn(indexResponseActionFuture);
    when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(dataSourceMetadata);

    verify(encryptor, times(1)).encrypt("secret_key");
    verify(encryptor, times(1)).encrypt("access_key");
    verify(client.admin().indices(), times(1)).create(any());
    verify(client, times(1)).index(any());
    verify(client.threadPool().getThreadContext(), times(2)).stashContext();


  }

  @Test
  public void testUpdateDataSourceMetadata() {
    when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.TRUE);
    when(encryptor.encrypt("secret_key")).thenReturn("secret_key");
    when(encryptor.encrypt("access_key")).thenReturn("access_key");
    when(client.update(any())).thenReturn(updateResponseActionFuture);
    when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.updateDataSourceMetadata(dataSourceMetadata);

    verify(encryptor, times(1)).encrypt("secret_key");
    verify(encryptor, times(1)).encrypt("access_key");
    verify(client.admin().indices(), times(0)).create(any());
    verify(client, times(1)).update(any());
    verify(client.threadPool().getThreadContext(), times(1)).stashContext();

  }

  @Test
  public void testDeleteDataSourceMetadata() {
    when(client.delete(any())).thenReturn(deleteResponseActionFuture);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);
    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.deleteDataSourceMetadata("testDS");

    verifyNoInteractions(encryptor);
    verify(client.admin().indices(), times(0)).create(any());
    verify(client, times(1)).delete(any());
    verify(client.threadPool().getThreadContext(), times(1)).stashContext();
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
