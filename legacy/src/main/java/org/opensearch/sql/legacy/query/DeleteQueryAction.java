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

package org.opensearch.sql.legacy.query;


import org.opensearch.client.Client;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder;
import org.opensearch.sql.legacy.domain.Delete;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.maker.QueryMaker;

public class DeleteQueryAction extends QueryAction {

    private final Delete delete;
    private DeleteByQueryRequestBuilder request;

    public DeleteQueryAction(Client client, Delete delete) {
        super(client, delete);
        this.delete = delete;
    }

    @Override
    public SqlElasticDeleteByQueryRequestBuilder explain() throws SqlParseException {
        this.request = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);

        setIndicesAndTypes();
        setWhere(delete.getWhere());
        SqlElasticDeleteByQueryRequestBuilder deleteByQueryRequestBuilder =
                new SqlElasticDeleteByQueryRequestBuilder(request);
        return deleteByQueryRequestBuilder;
    }


    /**
     * Set indices and types to the delete by query request.
     */
    private void setIndicesAndTypes() {

        DeleteByQueryRequest innerRequest = request.request();
        innerRequest.indices(query.getIndexArr());
        String[] typeArr = query.getTypeArr();
        if (typeArr != null) {
            innerRequest.getSearchRequest().types(typeArr);
        }
//        String[] typeArr = query.getTypeArr();
//        if (typeArr != null) {
//            request.set(typeArr);
//        }
    }


    /**
     * Create filters based on
     * the Where clause.
     *
     * @param where the 'WHERE' part of the SQL query.
     * @throws SqlParseException
     */
    private void setWhere(Where where) throws SqlParseException {
        if (where != null) {
            QueryBuilder whereQuery = QueryMaker.explain(where);
            request.filter(whereQuery);
        } else {
            request.filter(QueryBuilders.matchAllQuery());
        }
    }

}
