/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.query.multi;

import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;

/**
 * Created by Eliran on 19/8/2016.
 */
public class MultiQueryRequestBuilder implements SqlElasticRequestBuilder {

    private SearchRequestBuilder firstSearchRequest;
    private SearchRequestBuilder secondSearchRequest;
    private Map<String, String> firstTableFieldToAlias;
    private Map<String, String> secondTableFieldToAlias;
    private MultiQuerySelect multiQuerySelect;
    private SQLUnionOperator relation;


    public MultiQueryRequestBuilder(MultiQuerySelect multiQuerySelect) {
        this.multiQuerySelect = multiQuerySelect;
        this.relation = multiQuerySelect.getOperation();
        this.firstTableFieldToAlias = new HashMap<>();
        this.secondTableFieldToAlias = new HashMap<>();
    }

    @Override
    public ActionRequest request() {
        return null;
    }


    @Override
    public String explain() {

        try {
            XContentBuilder firstBuilder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            this.firstSearchRequest.request().source().toXContent(firstBuilder, ToXContent.EMPTY_PARAMS);

            XContentBuilder secondBuilder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            this.secondSearchRequest.request().source().toXContent(secondBuilder, ToXContent.EMPTY_PARAMS);
            return String.format("performing %s on :\n left query:\n%s\n right query:\n%s",
                    this.relation.name, BytesReference.bytes(firstBuilder).utf8ToString(),
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
        return null;
    }


    public SearchRequestBuilder getFirstSearchRequest() {
        return firstSearchRequest;
    }

    public SearchRequestBuilder getSecondSearchRequest() {
        return secondSearchRequest;
    }

    public SQLUnionOperator getRelation() {
        return relation;
    }

    public void setFirstSearchRequest(SearchRequestBuilder firstSearchRequest) {
        this.firstSearchRequest = firstSearchRequest;
    }

    public void setSecondSearchRequest(SearchRequestBuilder secondSearchRequest) {
        this.secondSearchRequest = secondSearchRequest;
    }

    public void fillTableAliases(List<Field> firstTableFields, List<Field> secondTableFields) {
        fillTableToAlias(this.firstTableFieldToAlias, firstTableFields);
        fillTableToAlias(this.secondTableFieldToAlias, secondTableFields);
    }

    private void fillTableToAlias(Map<String, String> fieldToAlias, List<Field> fields) {
        for (Field field : fields) {
            if (field.getAlias() != null && !field.getAlias().isEmpty()) {
                fieldToAlias.put(field.getName(), field.getAlias());
            }
        }
    }

    public Map<String, String> getFirstTableFieldToAlias() {
        return firstTableFieldToAlias;
    }

    public Map<String, String> getSecondTableFieldToAlias() {
        return secondTableFieldToAlias;
    }

    public Select getOriginalSelect(boolean first) {
        if (first) {
            return this.multiQuerySelect.getFirstSelect();
        } else {
            return this.multiQuerySelect.getSecondSelect();
        }
    }
}
