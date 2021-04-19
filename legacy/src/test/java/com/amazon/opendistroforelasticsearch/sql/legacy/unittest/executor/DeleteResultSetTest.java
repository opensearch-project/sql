/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.legacy.unittest.executor;

import com.amazon.opendistroforelasticsearch.sql.legacy.domain.Delete;
import com.amazon.opendistroforelasticsearch.sql.legacy.executor.format.DataRows;
import com.amazon.opendistroforelasticsearch.sql.legacy.executor.format.DeleteResultSet;
import com.amazon.opendistroforelasticsearch.sql.legacy.executor.format.Schema;
import org.opensearch.client.node.NodeClient;

import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;

import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.reindex.BulkByScrollResponse;

import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


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
