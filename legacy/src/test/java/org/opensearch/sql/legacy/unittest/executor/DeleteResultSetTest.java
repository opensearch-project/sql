/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.unittest.executor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.sql.legacy.domain.Delete;
import org.opensearch.sql.legacy.executor.format.DataRows;
import org.opensearch.sql.legacy.executor.format.DeleteResultSet;
import org.opensearch.sql.legacy.executor.format.Schema;


public class DeleteResultSetTest {

    @Mock
    NodeClient client;

    @Mock
    Delete deleteQuery;

    @Test
    public void testDeleteResponseForJdbcFormat() throws IOException {

        String jsonDeleteResponse = "{\n" +
            "  \"took\" : 73,\n" +
            "  \"timed_out\" : false,\n" +
            "  \"total\" : 1,\n" +
            "  \"updated\" : 0,\n" +
            "  \"created\" : 0,\n" +
            "  \"deleted\" : 10,\n" +
            "  \"batches\" : 1,\n" +
            "  \"version_conflicts\" : 0,\n" +
            "  \"noops\" : 0,\n" +
            "  \"retries\" : {\n" +
            "    \"bulk\" : 0,\n" +
            "    \"search\" : 0\n" +
            "  },\n" +
            "  \"throttled_millis\" : 0,\n" +
            "  \"requests_per_second\" : -1.0,\n" +
            "  \"throttled_until_millis\" : 0,\n" +
            "  \"failures\" : [ ]\n" +
            "}\n";

        XContentType xContentType = XContentType.JSON;
        XContentParser parser = xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            jsonDeleteResponse
        );

        BulkByScrollResponse deleteResponse  = BulkByScrollResponse.fromXContent(parser);
        DeleteResultSet deleteResultSet = new DeleteResultSet(client, deleteQuery, deleteResponse);
        Schema schema = deleteResultSet.getSchema();
        DataRows dataRows = deleteResultSet.getDataRows();

        assertThat(schema.getHeaders().size(), equalTo(1));
        assertThat(dataRows.getSize(), equalTo(1L));
        assertThat(dataRows.iterator().next().getData(DeleteResultSet.DELETED), equalTo(10L));
    }

}
