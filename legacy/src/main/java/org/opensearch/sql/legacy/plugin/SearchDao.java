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

package org.opensearch.sql.legacy.plugin;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.QueryActionRequest;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.query.QueryAction;


public class SearchDao {

    private static final Set<String> END_TABLE_MAP = new HashSet<>();

    static {
        END_TABLE_MAP.add("limit");
        END_TABLE_MAP.add("order");
        END_TABLE_MAP.add("where");
        END_TABLE_MAP.add("group");

    }

    private Client client = null;

    public SearchDao(Client client) {
        this.client = client;
    }

    public Client getClient() {
        return client;
    }

    /**
     * Prepare action And transform sql
     * into OpenSearch ActionRequest
     *
     * @param queryActionRequest SQL query action request to execute.
     * @return OpenSearch request
     * @throws SqlParseException
     */
    public QueryAction explain(QueryActionRequest queryActionRequest)
            throws SqlParseException, SQLFeatureNotSupportedException {
        return OpenSearchActionFactory.create(client, queryActionRequest);
    }
}
