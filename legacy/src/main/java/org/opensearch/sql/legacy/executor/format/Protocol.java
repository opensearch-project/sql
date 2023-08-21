/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import static org.opensearch.sql.legacy.domain.IndexStatement.StatementType;

import com.google.common.base.Strings;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.cursor.Cursor;
import org.opensearch.sql.legacy.cursor.NullCursor;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.domain.Delete;
import org.opensearch.sql.legacy.domain.IndexStatement;
import org.opensearch.sql.legacy.domain.Query;
import org.opensearch.sql.legacy.domain.QueryStatement;
import org.opensearch.sql.legacy.executor.adapter.QueryPlanQueryAction;
import org.opensearch.sql.legacy.executor.adapter.QueryPlanRequestBuilder;
import org.opensearch.sql.legacy.executor.format.DataRows.Row;
import org.opensearch.sql.legacy.executor.format.Schema.Column;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.planner.core.ColumnNode;

public class Protocol {

  static final int OK_STATUS = 200;
  static final int ERROR_STATUS = 500;

  private final String formatType;
  private int status;
  private long size;
  private long total;
  private ResultSet resultSet;
  private ErrorMessage error;
  private List<ColumnNode> columnNodeList;
  private Cursor cursor = new NullCursor();
  private ColumnTypeProvider scriptColumnType = new ColumnTypeProvider();

  public Protocol(
      Client client,
      QueryAction queryAction,
      Object queryResult,
      String formatType,
      Cursor cursor) {
    this.cursor = cursor;

    if (queryAction instanceof QueryPlanQueryAction) {
      this.columnNodeList =
          ((QueryPlanRequestBuilder) (((QueryPlanQueryAction) queryAction).explain()))
              .outputColumns();
    } else if (queryAction instanceof DefaultQueryAction) {
      scriptColumnType = queryAction.getScriptColumnType();
    }

    this.formatType = formatType;
    QueryStatement query = queryAction.getQueryStatement();
    this.status = OK_STATUS;
    this.resultSet = loadResultSet(client, query, queryResult);
    this.size = resultSet.getDataRows().getSize();
    this.total = resultSet.getDataRows().getTotalHits();
  }

  public Protocol(Client client, Object queryResult, String formatType, Cursor cursor) {
    this.cursor = cursor;
    this.status = OK_STATUS;
    this.formatType = formatType;
    this.resultSet = loadResultSetForCursor(client, queryResult);
  }

  public Protocol(Exception e) {
    this.formatType = null;
    this.status = ERROR_STATUS;
    this.error = ErrorMessageFactory.createErrorMessage(e, status);
  }

  private ResultSet loadResultSetForCursor(Client client, Object queryResult) {
    return new SelectResultSet(client, queryResult, formatType, cursor);
  }

  private ResultSet loadResultSet(
      Client client, QueryStatement queryStatement, Object queryResult) {
    if (queryResult instanceof List) {
      return new BindingTupleResultSet(columnNodeList, (List<BindingTuple>) queryResult);
    }
    if (queryStatement instanceof Delete) {
      return new DeleteResultSet(client, (Delete) queryStatement, queryResult);
    } else if (queryStatement instanceof Query) {
      return new SelectResultSet(
          client, (Query) queryStatement, queryResult, scriptColumnType, formatType, cursor);
    } else if (queryStatement instanceof IndexStatement) {
      IndexStatement statement = (IndexStatement) queryStatement;
      StatementType statementType = statement.getStatementType();

      if (statementType == StatementType.SHOW) {
        return new ShowResultSet(client, statement, queryResult);
      } else if (statementType == StatementType.DESCRIBE) {
        return new DescribeResultSet(client, statement, queryResult);
      }
    }

    throw new UnsupportedOperationException(
        String.format(
            "The following instance of QueryStatement is not supported: %s",
            queryStatement.getClass().toString()));
  }

  public int getStatus() {
    return status;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public String format() {
    if (status == OK_STATUS) {
      switch (formatType) {
        case "jdbc":
          return outputInJdbcFormat();
        case "table":
          return outputInTableFormat();
        case "raw":
          return outputInRawFormat();
        default:
          throw new UnsupportedOperationException(
              String.format("The following format is not supported: %s", formatType));
      }
    }

    return error.toString();
  }

  private String outputInJdbcFormat() {
    JSONObject formattedOutput = new JSONObject();

    formattedOutput.put("status", status);
    formattedOutput.put("size", size);
    formattedOutput.put("total", total);

    JSONArray schema = getSchemaAsJson();

    formattedOutput.put("schema", schema);
    formattedOutput.put("datarows", getDataRowsAsJson());

    String cursorId = cursor.generateCursorId();
    if (!Strings.isNullOrEmpty(cursorId)) {
      formattedOutput.put("cursor", cursorId);
    }

    return formattedOutput.toString(2);
  }

  private String outputInRawFormat() {
    Schema schema = resultSet.getSchema();
    DataRows dataRows = resultSet.getDataRows();

    StringBuilder formattedOutput = new StringBuilder();
    for (Row row : dataRows) {
      formattedOutput.append(rawEntry(row, schema)).append("\n");
    }

    return formattedOutput.toString();
  }

  private String outputInTableFormat() {
    return null;
  }

  public String cursorFormat() {
    if (status == OK_STATUS) {
      switch (formatType) {
        case "jdbc":
          return cursorOutputInJDBCFormat();
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "The following response format is not supported for cursor: [%s]", formatType));
      }
    }
    return error.toString();
  }

  private String cursorOutputInJDBCFormat() {
    JSONObject formattedOutput = new JSONObject();
    formattedOutput.put("datarows", getDataRowsAsJson());

    String cursorId = cursor.generateCursorId();
    if (!Strings.isNullOrEmpty(cursorId)) {
      formattedOutput.put("cursor", cursorId);
    }
    return formattedOutput.toString(2);
  }

  private String rawEntry(Row row, Schema schema) {
    // TODO String separator is being kept to "|" for the time being as using "\t" will require
    // formatting since
    // TODO tabs are occurring in multiple of 4 (one option is Guava's Strings.padEnd() method)
    return StreamSupport.stream(schema.spliterator(), false)
        .map(column -> row.getDataOrDefault(column.getName(), "NULL").toString())
        .collect(Collectors.joining("|"));
  }

  private JSONArray getSchemaAsJson() {
    Schema schema = resultSet.getSchema();
    JSONArray schemaJson = new JSONArray();

    for (Column column : schema) {
      schemaJson.put(schemaEntry(column.getName(), column.getAlias(), column.getType()));
    }

    return schemaJson;
  }

  private JSONObject schemaEntry(String name, String alias, String type) {
    JSONObject entry = new JSONObject();
    entry.put("name", name);
    if (alias != null) {
      entry.put("alias", alias);
    }
    entry.put("type", type);

    return entry;
  }

  private JSONArray getDataRowsAsJson() {
    Schema schema = resultSet.getSchema();
    DataRows dataRows = resultSet.getDataRows();
    JSONArray dataRowsJson = new JSONArray();

    for (Row row : dataRows) {
      dataRowsJson.put(dataEntry(row, schema));
    }

    return dataRowsJson;
  }

  private JSONArray dataEntry(Row dataRow, Schema schema) {
    JSONArray entry = new JSONArray();
    for (Column column : schema) {
      String columnName = column.getIdentifier();
      entry.put(dataRow.getDataOrDefault(columnName, JSONObject.NULL));
    }
    return entry;
  }
}
