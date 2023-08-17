/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.query.join;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;

/**
 * Created by Eliran on 15/9/2015.
 */
public class JoinRequestBuilder implements SqlElasticRequestBuilder {

    private MultiSearchRequest multi;
    private TableInJoinRequestBuilder firstTable;
    private TableInJoinRequestBuilder secondTable;
    private SQLJoinTableSource.JoinType joinType;
    private int totalLimit;

    public JoinRequestBuilder() {
        firstTable = new TableInJoinRequestBuilder();
        secondTable = new TableInJoinRequestBuilder();
    }


    @Override
    public ActionRequest request() {
        if (multi == null) {
            buildMulti();
        }
        return multi;

    }

    private void buildMulti() {
        multi = new MultiSearchRequest();
        multi.add(firstTable.getRequestBuilder());
        multi.add(secondTable.getRequestBuilder());
    }

    @Override
    public String explain() {
        try {
            XContentBuilder firstBuilder = XContentFactory.jsonBuilder().prettyPrint();
            firstTable.getRequestBuilder().request().source().toXContent(firstBuilder, ToXContent.EMPTY_PARAMS);

            XContentBuilder secondBuilder = XContentFactory.jsonBuilder().prettyPrint();
            secondTable.getRequestBuilder().request().source().toXContent(secondBuilder, ToXContent.EMPTY_PARAMS);
            return String.format(" first query:\n%s\n second query:\n%s",
                    BytesReference.bytes(firstBuilder).utf8ToString(),
                    BytesReference.bytes(secondBuilder).utf8ToString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ActionResponse get() {
        return null;
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return this.firstTable.getRequestBuilder();
    }

    public MultiSearchRequest getMulti() {
        return multi;
    }

    public void setMulti(MultiSearchRequest multi) {
        this.multi = multi;
    }

    public SQLJoinTableSource.JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(SQLJoinTableSource.JoinType joinType) {
        this.joinType = joinType;
    }

    public TableInJoinRequestBuilder getFirstTable() {
        return firstTable;
    }

    public TableInJoinRequestBuilder getSecondTable() {
        return secondTable;
    }

    public int getTotalLimit() {
        return totalLimit;
    }

    public void setTotalLimit(int totalLimit) {
        this.totalLimit = totalLimit;
    }

}
