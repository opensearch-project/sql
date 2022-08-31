/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage;

import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * S3 Storage Engine.
 */
@RequiredArgsConstructor
public class S3StorageEngine implements StorageEngine {

  private final QueryService queryService;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public Table getTable(String name) {
    try {
      UnresolvedPlan findS3Table = project(
          filter(
              relation(".extables"),
              function("=", qualifiedName("tableName"), stringLiteral(name))),
          alias("columns", qualifiedName("columns")),
          alias("fileFormat", qualifiedName("fileFormat")),
          alias("location", qualifiedName("location")));

      Map<String, ExprValue> tuple = queryService.execute(findS3Table)
          .getResults().get(0).tupleValue();
      String colJson = tuple.get("columns").stringValue();
      Map<String, String> colNameTypes = OBJECT_MAPPER.readValue(colJson,
          new TypeReference<HashMap<String, String>>() {});

      String fileFormat = tuple.get("fileFormat").stringValue();
      String location = tuple.get("location").stringValue();
      return new S3Table(name, colNameTypes, fileFormat, location);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
