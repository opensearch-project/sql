/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.visitor;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.INDEX;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.NESTED_FIELD;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.legacy.antlr.semantic.scope.Environment;
import org.opensearch.sql.legacy.antlr.semantic.scope.Namespace;
import org.opensearch.sql.legacy.antlr.semantic.scope.SemanticContext;
import org.opensearch.sql.legacy.antlr.semantic.scope.Symbol;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;
import org.opensearch.sql.legacy.antlr.visitor.EarlyExitAnalysisException;
import org.opensearch.sql.legacy.antlr.visitor.GenericSqlParseTreeVisitor;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.esdomain.mapping.FieldMappings;
import org.opensearch.sql.legacy.esdomain.mapping.IndexMappings;
import org.opensearch.sql.legacy.utils.StringUtils;

/** Load index and nested field mapping into semantic context */
public class OpenSearchMappingLoader implements GenericSqlParseTreeVisitor<Type> {

  /** Semantic context shared in the semantic analysis process */
  private final SemanticContext context;

  /** Local cluster state for mapping query */
  private final LocalClusterState clusterState;

  /** Threshold to decide if continue the analysis */
  private final int threshold;

  public OpenSearchMappingLoader(
      SemanticContext context, LocalClusterState clusterState, int threshold) {
    this.context = context;
    this.clusterState = clusterState;
    this.threshold = threshold;
  }

  /*
   * Suppose index 'accounts' includes 'name', 'age' and nested field 'projects'
   *  which includes 'name' and 'active'.
   *
   *  1. Define itself:
   *      ----- new definitions -----
   *      accounts -> INDEX
   *
   *  2. Define without alias no matter if alias given:
   *      'accounts' -> INDEX
   *      ----- new definitions -----
   *      'name' -> TEXT
   *      'age' -> INTEGER
   *      'projects' -> NESTED
   *      'projects.name' -> KEYWORD
   *      'projects.active' -> BOOLEAN
   */
  @Override
  public Type visitIndexName(String indexName) {
    if (isNotNested(indexName)) {
      defineIndexType(indexName);
      loadAllFieldsWithType(indexName);
    }
    return defaultValue();
  }

  @Override
  public void visitAs(String alias, Type type) {
    if (!(type instanceof OpenSearchIndex)) {
      return;
    }

    OpenSearchIndex index = (OpenSearchIndex) type;
    String indexName = type.getName();

    if (index.type() == INDEX) {
      String aliasName = alias.isEmpty() ? indexName : alias;
      defineAllFieldNamesByAppendingAliasPrefix(indexName, aliasName);
    } else if (index.type() == NESTED_FIELD) {
      if (!alias.isEmpty()) {
        defineNestedFieldNamesByReplacingWithAlias(indexName, alias);
      }
    } // else Do nothing for index pattern
  }

  private void defineIndexType(String indexName) {
    environment()
        .define(new Symbol(Namespace.FIELD_NAME, indexName), new OpenSearchIndex(indexName, INDEX));
  }

  private void loadAllFieldsWithType(String indexName) {
    Set<FieldMappings> mappings = getFieldMappings(indexName);
    mappings.forEach(mapping -> mapping.flat(this::defineFieldName));
  }

  /*
   *  3.1 Define with alias if given: ex."SELECT * FROM accounts a".
   *      'accounts' -> INDEX
   *      'name' -> TEXT
   *      'age' -> INTEGER
   *      'projects' -> NESTED
   *      'projects.name' -> KEYWORD
   *      'projects.active' -> BOOLEAN
   *      ----- new definitions -----
   *      ['a' -> INDEX]  -- this is done in semantic analyzer
   *      'a.name' -> TEXT
   *      'a.age' -> INTEGER
   *      'a.projects' -> NESTED
   *      'a.projects.name' -> KEYWORD
   *      'a.projects.active' -> BOOLEAN
   *
   *  3.2 Otherwise define by index full name: ex."SELECT * FROM account"
   *      'accounts' -> INDEX
   *      'name' -> TEXT
   *      'age' -> INTEGER
   *      'projects' -> NESTED
   *      'projects.name' -> KEYWORD
   *      'projects.active' -> BOOLEAN
   *      ----- new definitions -----
   *      'accounts.name' -> TEXT
   *      'accounts.age' -> INTEGER
   *      'accounts.projects' -> NESTED
   *      'accounts.projects.name' -> KEYWORD
   *      'accounts.projects.active' -> BOOLEAN
   */
  private void defineAllFieldNamesByAppendingAliasPrefix(String indexName, String alias) {
    Set<FieldMappings> mappings = getFieldMappings(indexName);
    mappings.stream()
        .forEach(
            mapping ->
                mapping.flat((fieldName, type) -> defineFieldName(alias + "." + fieldName, type)));
  }

  /*
   *  3.3 Define with alias if given: ex."SELECT * FROM accounts a, a.project p"
   *      'accounts' -> INDEX
   *      'name' -> TEXT
   *      'age' -> INTEGER
   *      'projects' -> NESTED
   *      'projects.name' -> KEYWORD
   *      'projects.active' -> BOOLEAN
   *      'a.name' -> TEXT
   *      'a.age' -> INTEGER
   *      'a.projects' -> NESTED
   *      'a.projects.name' -> KEYWORD
   *      'a.projects.active' -> BOOLEAN
   *      ----- new definitions -----
   *      ['p' -> NESTED] -- this is done in semantic analyzer
   *      'p.name' -> KEYWORD
   *      'p.active' -> BOOLEAN
   */
  private void defineNestedFieldNamesByReplacingWithAlias(String nestedFieldName, String alias) {
    Map<String, Type> typeByFullName =
        environment().resolveByPrefix(new Symbol(Namespace.FIELD_NAME, nestedFieldName));
    typeByFullName.forEach(
        (fieldName, fieldType) ->
            defineFieldName(fieldName.replace(nestedFieldName, alias), fieldType));
  }

  /**
   * Check if index name is NOT nested, for example. return true for index 'accounts' or
   * '.opensearch_dashboards' but return false for nested field name 'a.projects'.
   */
  private boolean isNotNested(String indexName) {
    return indexName.indexOf('.', 1) == -1; // taking care of .opensearch_dashboards
  }

  private Set<FieldMappings> getFieldMappings(String indexName) {
    IndexMappings indexMappings = clusterState.getFieldMappings(new String[] {indexName});
    Set<FieldMappings> fieldMappingsSet = new HashSet<>(indexMappings.allMappings());

    for (FieldMappings fieldMappings : fieldMappingsSet) {
      int size = fieldMappings.data().size();
      if (size > threshold) {
        throw new EarlyExitAnalysisException(
            StringUtils.format(
                "Index [%s] has [%d] fields more than threshold [%d]", indexName, size, threshold));
      }
    }
    return fieldMappingsSet;
  }

  private void defineFieldName(String fieldName, String type) {
    if ("NESTED".equalsIgnoreCase(type)) {
      defineFieldName(fieldName, new OpenSearchIndex(fieldName, NESTED_FIELD));
    } else {
      defineFieldName(fieldName, OpenSearchDataType.typeOf(type));
    }
  }

  private void defineFieldName(String fieldName, Type type) {
    Symbol symbol = new Symbol(Namespace.FIELD_NAME, fieldName);
    environment().define(symbol, type);
  }

  private Environment environment() {
    return context.peek();
  }
}
