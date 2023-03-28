/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import com.google.common.hash.HashCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.physical.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * This class is entry point to paged requests. It is responsible to cursor serialization
 * and deserialization.
 */
@RequiredArgsConstructor
public class PaginatedPlanCache {
  public static final String CURSOR_PREFIX = "n:";
  private final StorageEngine storageEngine;

  public boolean canConvertToCursor(UnresolvedPlan plan) {
    return plan.accept(new CanPaginateVisitor(), null);
  }

  /**
   * Converts a physical plan tree to a cursor. May cache plan related data somewhere.
   */
  public Cursor convertToCursor(PhysicalPlan plan) throws IOException {
    if (plan instanceof PaginateOperator) {
      var cursor = plan.toCursor();
      if (cursor == null) {
        return Cursor.None;
      }
      var raw = CURSOR_PREFIX + compress(cursor);
      return new Cursor(raw.getBytes());
    }
    return Cursor.None;
  }

  /**
   * Compress serialized query plan.
   * @param str string representing a query plan
   * @return str compressed with gzip.
   */
  String compress(String str) throws IOException {
    if (str == null || str.length() == 0) {
      return "";
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(str.getBytes());
    gzip.close();
    return HashCode.fromBytes(out.toByteArray()).toString();
  }

  /**
   * Decompresses a query plan that was compress with {@link PaginatedPlanCache#compress}.
   * @param input compressed query plan
   * @return decompressed string
   */
  String decompress(String input) throws IOException {
    if (input == null || input.length() == 0) {
      return "";
    }
    GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(
        HashCode.fromString(input).asBytes()));
    return new String(gzip.readAllBytes());
  }

  /**
   * Parse `NamedExpression`s from cursor.
   * @param listToFill List to fill with data.
   * @param cursor Cursor to parse.
   * @return Remaining part of the cursor.
   */
  private String parseNamedExpressions(List<NamedExpression> listToFill, String cursor) {
    var serializer = new DefaultExpressionSerializer();
    if (cursor.startsWith(")")) { //empty list
      return cursor.substring(cursor.indexOf(',') + 1);
    }
    while (!cursor.startsWith("(")) {
      listToFill.add((NamedExpression)
          serializer.deserialize(cursor.substring(0,
              Math.min(cursor.indexOf(','), cursor.indexOf(')')))));
      cursor = cursor.substring(cursor.indexOf(',') + 1);
    }
    return cursor;
  }

  /**
    * Converts a cursor to a physical plan tree.
    */
  public PhysicalPlan convertToPlan(String cursor) {
    if (!cursor.startsWith(CURSOR_PREFIX)) {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
    try {
      cursor = cursor.substring(CURSOR_PREFIX.length());
      cursor = decompress(cursor);

      // TODO Parse with ANTLR or serialize as JSON/XML
      if (!cursor.startsWith("(Paginate,")) {
        throw new UnsupportedOperationException("Unsupported cursor");
      }
      // TODO add checks for > 0
      cursor = cursor.substring(cursor.indexOf(',') + 1);
      final int currentPageIndex = Integer.parseInt(cursor, 0, cursor.indexOf(','), 10);

      cursor = cursor.substring(cursor.indexOf(',') + 1);
      final int pageSize = Integer.parseInt(cursor, 0, cursor.indexOf(','), 10);

      cursor = cursor.substring(cursor.indexOf(',') + 1);
      if (!cursor.startsWith("(Project,")) {
        throw new UnsupportedOperationException("Unsupported cursor");
      }
      cursor = cursor.substring(cursor.indexOf(',') + 1);
      if (!cursor.startsWith("(namedParseExpressions,")) {
        throw new UnsupportedOperationException("Unsupported cursor");
      }

      cursor = cursor.substring(cursor.indexOf(',') + 1);
      List<NamedExpression> namedParseExpressions = new ArrayList<>();
      cursor = parseNamedExpressions(namedParseExpressions, cursor);

      List<NamedExpression> projectList = new ArrayList<>();
      if (!cursor.startsWith("(projectList,")) {
        throw new UnsupportedOperationException("Unsupported cursor");
      }
      cursor = cursor.substring(cursor.indexOf(',') + 1);
      cursor = parseNamedExpressions(projectList, cursor);

      if (!cursor.startsWith("(OpenSearchPagedIndexScan,")) {
        throw new UnsupportedOperationException("Unsupported cursor");
      }
      cursor = cursor.substring(cursor.indexOf(',') + 1);
      var indexName = cursor.substring(0, cursor.indexOf(','));
      cursor = cursor.substring(cursor.indexOf(',') + 1);
      var scrollId = cursor.substring(0, cursor.indexOf(')'));
      TableScanOperator scan = storageEngine.getTableScan(indexName, scrollId);

      return new PaginateOperator(new ProjectOperator(scan, projectList, namedParseExpressions),
          pageSize, currentPageIndex);
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported cursor", e);
    }
  }
}
