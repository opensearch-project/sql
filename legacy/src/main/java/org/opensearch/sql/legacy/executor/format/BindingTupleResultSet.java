/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.executor.format;

import static org.opensearch.sql.legacy.executor.format.DateFieldFormatter.FORMAT_JDBC;

import com.google.common.annotations.VisibleForTesting;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.expression.model.ExprValue;
import org.opensearch.sql.legacy.query.planner.core.ColumnNode;

/**
 * The definition of BindingTuple ResultSet.
 */
public class BindingTupleResultSet extends ResultSet {

    public BindingTupleResultSet(List<ColumnNode> columnNodes, List<BindingTuple> bindingTuples) {
        this.schema = buildSchema(columnNodes);
        this.dataRows = buildDataRows(columnNodes, bindingTuples);
    }

    @VisibleForTesting
    public static Schema buildSchema(List<ColumnNode> columnNodes) {
        List<Schema.Column> columnList = columnNodes.stream()
                                                 .map(node -> new Schema.Column(
                                                         node.getName(),
                                                         node.getAlias(),
                                                         node.getType()))
                                                 .collect(Collectors.toList());
        return new Schema(columnList);
    }

    @VisibleForTesting
    public static DataRows buildDataRows(List<ColumnNode> columnNodes, List<BindingTuple> bindingTuples) {
        List<DataRows.Row> rowList = bindingTuples.stream().map(tuple -> {
            Map<String, ExprValue> bindingMap = tuple.getBindingMap();
            Map<String, Object> rowMap = new HashMap<>();
            for (ColumnNode column : columnNodes) {
                String columnName = column.columnName();
                Object value = bindingMap.get(columnName).value();
                if (column.getType() == Schema.Type.DATE) {
                    value = DateFormat.getFormattedDate(new Date((Long) value), FORMAT_JDBC);
                }
                rowMap.put(columnName, value);
            }
            return new DataRows.Row(rowMap);
        }).collect(Collectors.toList());

        return new DataRows(bindingTuples.size(), bindingTuples.size(), rowList);
    }
}
