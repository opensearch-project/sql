/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.util;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.search.builder.SearchSourceBuilder.ScriptField;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.mockito.stubbing.Answer;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.parser.ElasticSqlExprParser;
import org.opensearch.sql.legacy.parser.ScriptFilter;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

public class CheckScriptContents {

    private static SQLExpr queryToExpr(String query) {
        return new ElasticSqlExprParser(query).expr();
    }

    public static ScriptField getScriptFieldFromQuery(String query) {
        try {
            Client mockClient = mock(Client.class);
            stubMockClient(mockClient);
            QueryAction queryAction = OpenSearchActionFactory.create(mockClient, query);
            SqlElasticRequestBuilder requestBuilder = queryAction.explain();

            SearchRequestBuilder request = (SearchRequestBuilder) requestBuilder.getBuilder();
            List<ScriptField> scriptFields = request.request().source().scriptFields();

            assertTrue(scriptFields.size() == 1);

            return scriptFields.get(0);

        } catch (SQLFeatureNotSupportedException | SqlParseException | SQLFeatureDisabledException e) {
            throw new ParserException("Unable to parse query: " + query, e);
        }
    }

    public static ScriptFilter getScriptFilterFromQuery(String query, SqlParser parser) {
        try {
            Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
            Where where = select.getWhere();

            assertTrue(where.getWheres().size() == 1);
            assertTrue(((Condition) (where.getWheres().get(0))).getValue() instanceof ScriptFilter);

            return (ScriptFilter) (((Condition) (where.getWheres().get(0))).getValue());

        } catch (SqlParseException e) {
            throw new ParserException("Unable to parse query: " + query);
        }
    }

    public static boolean scriptContainsString(ScriptField scriptField, String string) {
        return scriptField.script().getIdOrCode().contains(string);
    }

    public static boolean scriptContainsString(ScriptFilter scriptFilter, String string) {
        return scriptFilter.getScript().contains(string);
    }

    public static boolean scriptHasPattern(ScriptField scriptField, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(scriptField.script().getIdOrCode());
        return matcher.find();
    }

    public static boolean scriptHasPattern(ScriptFilter scriptFilter, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(scriptFilter.getScript());
        return matcher.find();
    }

    public static void stubMockClient(Client mockClient) {
            String mappings = "{\n" +
                "  \"opensearch-sql_test_index_bank\": {\n" +
                "    \"mappings\": {\n" +
                "      \"account\": {\n" +
                "        \"properties\": {\n" +
                "          \"account_number\": {\n" +
                "            \"type\": \"long\"\n" +
                "          },\n" +
                "          \"address\": {\n" +
                "            \"type\": \"text\"\n" +
                "          },\n" +
                "          \"age\": {\n" +
                "            \"type\": \"integer\"\n" +
                "          },\n" +
                "          \"balance\": {\n" +
                "            \"type\": \"long\"\n" +
                "          },\n" +
                "          \"birthdate\": {\n" +
                "            \"type\": \"date\"\n" +
                "          },\n" +
                "          \"city\": {\n" +
                "            \"type\": \"keyword\"\n" +
                "          },\n" +
                "          \"email\": {\n" +
                "            \"type\": \"text\"\n" +
                "          },\n" +
                "          \"employer\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fields\": {\n" +
                "              \"keyword\": {\n" +
                "                \"type\": \"keyword\",\n" +
                "                \"ignore_above\": 256\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          \"firstname\": {\n" +
                "            \"type\": \"text\"\n" +
                "          },\n" +
                "          \"gender\": {\n" +
                "            \"type\": \"text\"\n" +
                "          },\n" +
                "          \"lastname\": {\n" +
                "            \"type\": \"keyword\"\n" +
                "          },\n" +
                "          \"male\": {\n" +
                "            \"type\": \"boolean\"\n" +
                "          },\n" +
                "          \"state\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fields\": {\n" +
                "              \"raw\": {\n" +
                "                \"type\": \"keyword\",\n" +
                "                \"ignore_above\": 256\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                // ==== All required by IndexMetaData.fromXContent() ====
                "    \"settings\": {\n" +
                "      \"index\": {\n" +
                "        \"number_of_shards\": 5,\n" +
                "        \"number_of_replicas\": 0,\n" +
                "        \"version\": {\n" +
                "          \"created\": \"6050399\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"mapping_version\": \"1\",\n" +
                "    \"settings_version\": \"1\"\n" +
                //=======================================================
                "  }\n" +
                "}";

            AdminClient mockAdminClient = mock(AdminClient.class);
            when(mockClient.admin()).thenReturn(mockAdminClient);

            IndicesAdminClient mockIndexClient = mock(IndicesAdminClient.class);
            when(mockAdminClient.indices()).thenReturn(mockIndexClient);

            ActionFuture<GetFieldMappingsResponse> mockActionResp = mock(ActionFuture.class);
            when(mockIndexClient.getFieldMappings(any(GetFieldMappingsRequest.class))).thenReturn(mockActionResp);
            mockLocalClusterState(mappings);
    }

    public static XContentParser createParser(String mappings) throws IOException {
        return XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            mappings
        );
    }

    public static void mockLocalClusterState(String mappings) {
        LocalClusterState.state().setClusterService(mockClusterService(mappings));
        LocalClusterState.state().setResolver(mockIndexNameExpressionResolver());
        LocalClusterState.state().setPluginSettings(mockPluginSettings());
    }

    public static ClusterService mockClusterService(String mappings) {
        ClusterService mockService = mock(ClusterService.class);
        ClusterState mockState = mock(ClusterState.class);
        Metadata mockMetaData = mock(Metadata.class);

        when(mockService.state()).thenReturn(mockState);
        when(mockState.metadata()).thenReturn(mockMetaData);
        try {
            when(mockMetaData.findMappings(any(),  any())).thenReturn(
                Map.of(TestsConstants.TEST_INDEX_BANK, IndexMetadata.fromXContent(
                    createParser(mappings)).mapping()));
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return mockService;
    }

    public static IndexNameExpressionResolver mockIndexNameExpressionResolver() {
        IndexNameExpressionResolver mockResolver = mock(IndexNameExpressionResolver.class);
        when(mockResolver.concreteIndexNames(any(), any(), anyBoolean(), anyString())).thenAnswer(
            (Answer<String[]>) invocation -> {
                // Return index expression directly without resolving
                Object indexExprs = invocation.getArguments()[3];
                if (indexExprs instanceof String) {
                    return new String[]{ (String) indexExprs };
                }
                return (String[]) indexExprs;
            }
        );
        return mockResolver;
    }

    public static OpenSearchSettings mockPluginSettings() {
        OpenSearchSettings settings = mock(OpenSearchSettings.class);

        // Force return empty list to avoid ClusterSettings be invoked which is a final class and hard to mock.
        // In this case, default value in Setting will be returned all the time.
        doReturn(emptyList()).when(settings).getSettings();
        return settings;
    }

}
