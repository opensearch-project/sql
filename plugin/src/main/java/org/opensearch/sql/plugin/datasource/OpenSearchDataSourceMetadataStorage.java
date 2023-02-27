/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.datasource.DataSourceMetadataStorage;
import org.opensearch.sql.datasource.encryptor.CredentialInfoEncryptor;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.auth.AuthenticationType;
import org.opensearch.sql.opensearch.security.SecurityAccess;

public class OpenSearchDataSourceMetadataStorage implements DataSourceMetadataStorage {

  private static final String DATASOURCE_INDEX_NAME = ".ql-datasources";
  private static final String DATASOURCE_INDEX_MAPPING_FILE_NAME = "datasources-index-mapping.yml";
  private static final String DATASOURCE_INDEX_SETTINGS_FILE_NAME
      = "datasources-index-settings.yml";
  private static String DATASOURCE_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";
  private static final Logger LOG = LogManager.getLogger();

  private final Client client;
  private final ClusterService clusterService;
  private final CredentialInfoEncryptor credentialInfoEncryptor;
  
  public OpenSearchDataSourceMetadataStorage(Client client, ClusterService clusterService,
                                             CredentialInfoEncryptor credentialInfoEncryptor) {
    this.client = client;
    this.clusterService = clusterService;
    this.credentialInfoEncryptor = credentialInfoEncryptor;
  }

  @Override
  public List<DataSourceMetadata> getDataSourceMetadata() {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    return searchInElasticSearch(QueryBuilders.matchAllQuery());
  }

  @Override
  public Optional<DataSourceMetadata> getDataSourceMetadata(String datasourceName) {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    return searchInElasticSearch(QueryBuilders.termQuery("name", datasourceName))
        .stream()
        .findFirst()
        .map(this::decryptConfidentialInfo);
  }

  private DataSourceMetadata decryptConfidentialInfo(DataSourceMetadata dataSourceMetadata) {
    Map<String, String> propertiesMap = dataSourceMetadata.getProperties();
    Optional<AuthenticationType> authTypeOptional
        = propertiesMap.keySet().stream().filter(s -> s.endsWith("auth.type"))
        .findFirst()
        .map(propertiesMap::get)
        .map(AuthenticationType::get);
    switch (authTypeOptional.get()) {
      case BASICAUTH:
        Optional<String> passwordKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.password"))
            .findFirst();
        propertiesMap.put(passwordKey.get(),
            this.credentialInfoEncryptor.decrypt(propertiesMap.get(passwordKey.get())));
        break;
      case AWSSIGV4AUTH:
        Optional<String> accessKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.access_key"))
            .findFirst();
        propertiesMap.put(accessKey.get(),
            this.credentialInfoEncryptor.decrypt(propertiesMap.get(accessKey.get())));
        Optional<String> secretKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.secret_key"))
            .findFirst();
        propertiesMap.put(secretKey.get(),
            this.credentialInfoEncryptor.decrypt(propertiesMap.get(secretKey.get())));
        break;
      default:
        break;
    }
    return dataSourceMetadata;
  }

  @Override
  public void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {
    validateDataSourceMetaData(dataSourceMetadata);
    encryptConfidentialData(dataSourceMetadata);
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    IndexRequest indexRequest = new IndexRequest(DATASOURCE_INDEX_NAME);
    indexRequest.id(dataSourceMetadata.getName());
    indexRequest.create(true);
    ObjectMapper objectMapper = new ObjectMapper();
    ActionFuture<IndexResponse> indexResponseActionFuture = SecurityAccess.doPrivileged(() -> {
        indexRequest.source(objectMapper.writeValueAsString(dataSourceMetadata), XContentType.JSON);
        return client.index(indexRequest);
    });
    IndexResponse indexResponse = indexResponseActionFuture.actionGet();
    if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
      LOG.debug("DatasourceMetadata : {}  successfully created", dataSourceMetadata);
    }
  }

  private void createDataSourcesIndex() {
    try {
      InputStream mappingFileStream = OpenSearchDataSourceMetadataStorage.class.getClassLoader()
          .getResourceAsStream(DATASOURCE_INDEX_MAPPING_FILE_NAME);
      InputStream settingsFileStream = OpenSearchDataSourceMetadataStorage.class.getClassLoader()
          .getResourceAsStream(DATASOURCE_INDEX_SETTINGS_FILE_NAME);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(DATASOURCE_INDEX_NAME);
      createIndexRequest
          .mapping(IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8),
              XContentType.YAML)
          .settings(IOUtils.toString(settingsFileStream, StandardCharsets.UTF_8),
              XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture
          = client.admin().indices().create(createIndexRequest);
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();
      if (createIndexResponse.isAcknowledged()) {
        LOG.info("Index: {} creation Acknowledged", DATASOURCE_INDEX_NAME);
      } else {
        throw new IllegalStateException(
            String.format("Index: %s creation not acknowledged", DATASOURCE_INDEX_NAME ));
      }
    } catch (Throwable e) {
      throw new RuntimeException(
          "Internal server error while creating" + DATASOURCE_INDEX_NAME + " index");
    }
  }

  private void encryptConfidentialData(DataSourceMetadata dataSourceMetadata) {
    Map<String, String> propertiesMap = dataSourceMetadata.getProperties();
    Optional<AuthenticationType> authTypeOptional
        = propertiesMap.keySet().stream().filter(s -> s.endsWith("auth.type"))
        .findFirst()
        .map(propertiesMap::get)
        .map(AuthenticationType::get);
    switch (authTypeOptional.get()) {
      case BASICAUTH:
        Optional<String> passwordKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.password"))
            .findFirst();
        propertiesMap.put(passwordKey.get(),
            this.credentialInfoEncryptor.encrypt(propertiesMap.get(passwordKey.get())).toString());
        break;
      case AWSSIGV4AUTH:
        Optional<String> accessKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.access_key"))
            .findFirst();
        propertiesMap.put(accessKey.get(),
            this.credentialInfoEncryptor.encrypt(propertiesMap.get(accessKey.get())));
        Optional<String> secretKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.secret_key"))
            .findFirst();
        propertiesMap.put(secretKey.get(),
            this.credentialInfoEncryptor.encrypt(propertiesMap.get(secretKey.get())));
        break;

      default:
        break;
    }
  }


  /**
   * This can be moved to a different validator class when we introduce more connectors.
   *
   * @param metadata {@link DataSourceMetadata}.
   */
  private void validateDataSourceMetaData(DataSourceMetadata metadata) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(metadata.getName()),
        "Missing Name Field from a DataSource. Name is a required parameter.");
    Preconditions.checkArgument(
        metadata.getName().matches(DATASOURCE_NAME_REGEX),
        StringUtils.format(
            "DataSource Name: %s contains illegal characters. Allowed characters: a-zA-Z0-9_-*@.",
            metadata.getName()));
    Preconditions.checkArgument(
        !Objects.isNull(metadata.getProperties()),
        "Missing properties field in catalog configuration. Properties are required parameters.");
  }


  private List<DataSourceMetadata> searchInElasticSearch(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(DATASOURCE_INDEX_NAME);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture = client.search(searchRequest);
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException("Internal server error while fetching datasource metadata information");
    } else {
      ObjectMapper objectMapper = new ObjectMapper();
      return Arrays.stream(searchResponse
              .getHits()
              .getHits())
          .map(SearchHit::getSourceAsString)
          .map(datasourceString
              -> SecurityAccess.doPrivileged(() -> objectMapper.readValue(datasourceString, DataSourceMetadata.class)))
          .collect(Collectors.toList());
    }
  }

}
