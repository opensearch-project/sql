/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.join.aggregations.JoinAggregationBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Having;
import org.opensearch.sql.legacy.domain.KVValue;
import org.opensearch.sql.legacy.domain.MethodField;
import org.opensearch.sql.legacy.domain.Order;
import org.opensearch.sql.legacy.domain.ScriptMethodField;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.maker.AggMaker;
import org.opensearch.sql.legacy.query.maker.QueryMaker;
import org.opensearch.transport.client.Client;

/** Transform SQL query to OpenSearch aggregations query */
public class AggregationQueryAction extends QueryAction {

  private final Select select;
  private final AggMaker aggMaker = new AggMaker();
  private SearchRequestBuilder request;

  public AggregationQueryAction(Client client, Select select) {
    super(client, select);
    this.select = select;
  }

  @Override
  public SqlOpenSearchRequestBuilder explain() throws SqlParseException {
    this.request = new SearchRequestBuilder(client, SearchAction.INSTANCE);

    if (select.getRowCount() == null) {
      select.setRowCount(Select.DEFAULT_LIMIT);
    }

    setIndicesAndTypes();

    setWhere(select.getWhere());
    AggregationBuilder lastAgg = null;

    for (List<Field> groupBy : select.getGroupBys()) {
      if (!groupBy.isEmpty()) {
        Field field = groupBy.get(0);

        // make groupby can reference to field alias
        lastAgg = getGroupAgg(field, select);

        if (lastAgg instanceof TermsAggregationBuilder) {

          // TODO: Consider removing that condition
          // in theory we should be able to apply this for all types of fields, but
          // this change requires too much of related integration tests (e.g. there are comparisons
          // against
          // raw javascript dsl, so I'd like to scope the changes as of now to one particular fix
          // for
          // scripted functions

          // the condition `field.getName().equals("script")` is to include the CAST cases, since
          // the cast
          // method is instance of MethodField with script. => corrects the shard size of CASTs
          if (!(field instanceof MethodField)
              || field instanceof ScriptMethodField
              || field.getName().equals("script")) {
            // if limit size is too small, increasing shard  size is required
            if (select.getRowCount() < 200) {
              ((TermsAggregationBuilder) lastAgg).shardSize(2000);
              for (Hint hint : select.getHints()) {
                if (hint.getType() == HintType.SHARD_SIZE) {
                  if (hint.getParams() != null
                      && hint.getParams().length != 0
                      && hint.getParams()[0] != null) {
                    ((TermsAggregationBuilder) lastAgg).shardSize((Integer) hint.getParams()[0]);
                  }
                }
              }
            }

            if (select.getRowCount() > 0) {
              ((TermsAggregationBuilder) lastAgg).size(select.getRowCount());
            }
          }
        }

        if (field.isNested()) {
          AggregationBuilder nestedBuilder = createNestedAggregation(field);

          if (insertFilterIfExistsAfter(lastAgg, groupBy, nestedBuilder, 1)) {
            groupBy.remove(1);
          } else {
            nestedBuilder.subAggregation(lastAgg);
          }

          request.addAggregation(wrapNestedIfNeeded(nestedBuilder, field.isReverseNested()));
        } else if (field.isChildren()) {
          AggregationBuilder childrenBuilder = createChildrenAggregation(field);

          if (insertFilterIfExistsAfter(lastAgg, groupBy, childrenBuilder, 1)) {
            groupBy.remove(1);
          } else {
            childrenBuilder.subAggregation(lastAgg);
          }

          request.addAggregation(childrenBuilder);
        } else {
          request.addAggregation(lastAgg);
        }

        for (int i = 1; i < groupBy.size(); i++) {
          field = groupBy.get(i);
          AggregationBuilder subAgg = getGroupAgg(field, select);
          // ES5.0 termsaggregation with size = 0 not supported anymore
          //                    if (subAgg instanceof TermsAggregationBuilder && !(field instanceof
          // MethodField)) {

          //                        //((TermsAggregationBuilder) subAgg).size(0);
          //                    }

          if (field.isNested()) {
            AggregationBuilder nestedBuilder = createNestedAggregation(field);

            if (insertFilterIfExistsAfter(subAgg, groupBy, nestedBuilder, i + 1)) {
              groupBy.remove(i + 1);
              i++;
            } else {
              nestedBuilder.subAggregation(subAgg);
            }

            lastAgg.subAggregation(wrapNestedIfNeeded(nestedBuilder, field.isReverseNested()));
          } else if (field.isChildren()) {
            AggregationBuilder childrenBuilder = createChildrenAggregation(field);

            if (insertFilterIfExistsAfter(subAgg, groupBy, childrenBuilder, i + 1)) {
              groupBy.remove(i + 1);
              i++;
            } else {
              childrenBuilder.subAggregation(subAgg);
            }

            lastAgg.subAggregation(childrenBuilder);
          } else {
            lastAgg.subAggregation(subAgg);
          }

          lastAgg = subAgg;
        }
      }

      // explain the field from SELECT and HAVING clause
      List<Field> combinedList = new ArrayList<>();
      combinedList.addAll(select.getFields());
      if (select.getHaving() != null) {
        combinedList.addAll(select.getHaving().getHavingFields());
      }
      // add aggregation function to each groupBy
      explanFields(request, combinedList, lastAgg);

      explainHaving(lastAgg);
    }

    if (select.getGroupBys().size() < 1) {
      // add aggregation when having no groupBy script
      explanFields(request, select.getFields(), lastAgg);
    }

    Map<String, KVValue> groupMap = aggMaker.getGroupMap();
    // add field
    if (select.getFields().size() > 0) {
      setFields(select.getFields());
      //            explanFields(request, select.getFields(), lastAgg);
    }

    // add order
    if (lastAgg != null && select.getOrderBys().size() > 0) {
      for (Order order : select.getOrderBys()) {

        // check "standard" fields
        KVValue temp = groupMap.get(order.getName());
        if (temp != null) {
          TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) temp.value;
          switch (temp.key) {
            case "COUNT":
              termsBuilder.order(BucketOrder.count(isASC(order)));
              break;
            case "KEY":
              termsBuilder.order(BucketOrder.key(isASC(order)));
              break;
            case "FIELD":
              termsBuilder.order(BucketOrder.aggregation(order.getName(), isASC(order)));
              break;
            default:
              throw new SqlParseException(order.getName() + " can not to order");
          }
        } else if (order.isScript()) {
          // Do not add scripted fields into sort, they must be sorted inside of aggregation
        } else {
          // TODO: Is there a legit case when we want to add field into sort for aggregation
          // queries?
          request.addSort(order.getName(), SortOrder.valueOf(order.getType()));
        }
      }
    }

    setLimitFromHint(this.select.getHints());

    request.setSearchType(SearchType.DEFAULT);
    updateRequestWithIndexAndRoutingOptions(select, request);
    updateRequestWithHighlight(select, request);
    updateRequestWithCollapse(select, request);
    updateRequestWithPostFilter(select, request);
    return new SqlOpenSearchRequestBuilder(request);
  }

  private AggregationBuilder getGroupAgg(Field groupByField, Select select)
      throws SqlParseException {
    AggregationBuilder lastAgg = null;
    Field shadowField = null;

    for (Field selectField : select.getFields()) {
      if (selectField instanceof MethodField && selectField.getName().equals("script")) {
        MethodField scriptField = (MethodField) selectField;
        for (KVValue kv : scriptField.getParams()) {
          if (kv.value.equals(groupByField.getName())) {
            shadowField = scriptField;
            break;
          }
        }
      }
    }

    if (shadowField == null) {
      for (Field selectField : select.getFields()) {
        if (selectField.getAlias() != null
            && (groupByField.getName().equals(selectField.getAlias())
                || groupByField.getExpression().equals(selectField.getExpression()))) {
          shadowField = selectField;
        }
      }
    }

    if (null != shadowField) {
      groupByField.setAlias(shadowField.getAlias());
      groupByField = shadowField;
    }

    lastAgg = aggMaker.makeGroupAgg(groupByField);

    // find if we have order for that aggregation. As of now only special case for script fields
    if (groupByField.isScriptField()) {
      addOrderByScriptFieldIfPresent(
          select, (TermsAggregationBuilder) lastAgg, groupByField.getExpression());
    }

    return lastAgg;
  }

  private void addOrderByScriptFieldIfPresent(
      Select select, TermsAggregationBuilder groupByAggregation, SQLExpr groupByExpression) {
    // TODO: Explore other ways to correlate different fields/functions in the query (params?)
    // This feels like a hacky way, but it's the best that could be done now.
    select.getOrderBys().stream()
        .filter(order -> groupByExpression.equals(order.getSortField().getExpression()))
        .findFirst()
        .ifPresent(
            orderForGroupBy -> groupByAggregation.order(BucketOrder.key(isASC(orderForGroupBy))));
  }

  private AggregationBuilder wrapNestedIfNeeded(
      AggregationBuilder nestedBuilder, boolean reverseNested) {
    if (!reverseNested) {
      return nestedBuilder;
    }
    if (reverseNested && !(nestedBuilder instanceof NestedAggregationBuilder)) {
      return nestedBuilder;
    }
    // we need to jump back to root
    return AggregationBuilders.reverseNested(nestedBuilder.getName() + "_REVERSED")
        .subAggregation(nestedBuilder);
  }

  private AggregationBuilder createNestedAggregation(Field field) {
    AggregationBuilder nestedBuilder;

    String nestedPath = field.getNestedPath();

    if (field.isReverseNested()) {
      if (nestedPath == null || !nestedPath.startsWith("~")) {
        ReverseNestedAggregationBuilder reverseNestedAggregationBuilder =
            AggregationBuilders.reverseNested(getNestedAggName(field));
        if (nestedPath != null) {
          reverseNestedAggregationBuilder.path(nestedPath);
        }
        return reverseNestedAggregationBuilder;
      }
      nestedPath = nestedPath.substring(1);
    }

    nestedBuilder = AggregationBuilders.nested(getNestedAggName(field), nestedPath);

    return nestedBuilder;
  }

  private AggregationBuilder createChildrenAggregation(Field field) {
    AggregationBuilder childrenBuilder;

    String childType = field.getChildType();

    childrenBuilder = JoinAggregationBuilders.children(getChildrenAggName(field), childType);

    return childrenBuilder;
  }

  private String getNestedAggName(Field field) {
    String prefix;

    if (field instanceof MethodField) {
      String nestedPath = field.getNestedPath();
      if (nestedPath != null) {
        prefix = nestedPath;
      } else {
        prefix = field.getAlias();
      }
    } else {
      prefix = field.getName();
    }
    return prefix + "@NESTED";
  }

  private String getChildrenAggName(Field field) {
    String prefix;

    if (field instanceof MethodField) {
      String childType = field.getChildType();

      if (childType != null) {
        prefix = childType;
      } else {
        prefix = field.getAlias();
      }
    } else {
      prefix = field.getName();
    }

    return prefix + "@CHILDREN";
  }

  private boolean insertFilterIfExistsAfter(
      AggregationBuilder agg, List<Field> groupBy, AggregationBuilder builder, int nextPosition)
      throws SqlParseException {
    if (groupBy.size() <= nextPosition) {
      return false;
    }
    Field filterFieldCandidate = groupBy.get(nextPosition);
    if (!(filterFieldCandidate instanceof MethodField)) {
      return false;
    }
    MethodField methodField = (MethodField) filterFieldCandidate;
    if (!methodField.getName().toLowerCase().equals("filter")) {
      return false;
    }
    builder.subAggregation(aggMaker.makeGroupAgg(filterFieldCandidate).subAggregation(agg));
    return true;
  }

  private AggregationBuilder updateAggIfNested(AggregationBuilder lastAgg, Field field) {
    if (field.isNested()) {
      lastAgg =
          AggregationBuilders.nested(field.getName() + "Nested", field.getNestedPath())
              .subAggregation(lastAgg);
    }
    return lastAgg;
  }

  private boolean isASC(Order order) {
    return "ASC".equals(order.getType());
  }

  private void setFields(List<Field> fields) {
    if (select.getFields().size() > 0) {
      ArrayList<String> includeFields = new ArrayList<>();

      for (Field field : fields) {
        if (field != null) {
          includeFields.add(field.getName());
        }
      }

      request.setFetchSource(includeFields.toArray(new String[0]), null);
    }
  }

  private void explanFields(
      SearchRequestBuilder request, List<Field> fields, AggregationBuilder groupByAgg)
      throws SqlParseException {
    for (Field field : fields) {
      if (field instanceof MethodField) {

        if (field.getName().equals("script")) {
          request.addStoredField(field.getAlias());
          DefaultQueryAction defaultQueryAction = new DefaultQueryAction(client, select);
          defaultQueryAction.initialize(request);
          List<Field> tempFields = Lists.newArrayList(field);
          defaultQueryAction.setFields(tempFields);
          continue;
        }

        AggregationBuilder makeAgg =
            aggMaker.withWhere(select.getWhere()).makeFieldAgg((MethodField) field, groupByAgg);
        if (groupByAgg != null) {
          groupByAgg.subAggregation(makeAgg);
        } else {
          request.addAggregation(makeAgg);
        }
      } else if (field != null) {
        request.addStoredField(field.getName());
      } else {
        throw new SqlParseException("it did not support this field method " + field);
      }
    }
  }

  private void explainHaving(AggregationBuilder lastAgg) throws SqlParseException {
    Having having = select.getHaving();
    if (having != null) {
      having.explain(lastAgg, select.getFields());
    }
  }

  /**
   * Create filters based on the Where clause.
   *
   * @param where the 'WHERE' part of the SQL query.
   * @throws SqlParseException
   */
  private void setWhere(Where where) throws SqlParseException {
    BoolQueryBuilder boolQuery = null;
    if (where != null) {
      boolQuery = QueryMaker.explain(where, this.select.isQuery);
    }
    // Used to prevent NullPointerException in old tests as they do not set sqlRequest in
    // QueryAction
    if (sqlRequest != null) {
      boolQuery = sqlRequest.checkAndAddFilter(boolQuery);
    }
    request.setQuery(boolQuery);
  }

  /** Set indices and types to the search request. */
  private void setIndicesAndTypes() {
    request.setIndices(query.getIndexArr());
  }

  private void setLimitFromHint(List<Hint> hints) {
    int from = 0;
    int size = 0;
    for (Hint hint : hints) {
      if (hint.getType() == HintType.DOCS_WITH_AGGREGATION) {
        Integer[] params = (Integer[]) hint.getParams();
        if (params.length > 1) {
          // if 2 or more are given, use the first as the from and the second as the size
          // so it is the same as LIMIT from,size
          // except written as /*! DOCS_WITH_AGGREGATION(from,size) */
          from = params[0];
          size = params[1];
        } else if (params.length == 1) {
          // if only 1 parameter is given, use it as the size with a from of 0
          size = params[0];
        }
        break;
      }
    }
    request.setFrom(from);
    request.setSize(size);
  }
}
