/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.visitor;

import java.util.List;

/** Generic parse tree visitor without dependency on concrete parse tree class. */
public interface GenericSqlParseTreeVisitor<T> {

  default void visitRoot() {}

  default void visitQuery() {}

  default void endVisitQuery() {}

  default T visitSelect(List<T> items) {
    return defaultValue();
  }

  default T visitSelectAllColumn() {
    return defaultValue();
  }

  default void visitAs(String alias, T type) {}

  default T visitIndexName(String indexName) {
    return defaultValue();
  }

  default T visitFieldName(String fieldName) {
    return defaultValue();
  }

  default T visitFunctionName(String funcName) {
    return defaultValue();
  }

  default T visitOperator(String opName) {
    return defaultValue();
  }

  default T visitString(String text) {
    return defaultValue();
  }

  default T visitInteger(String text) {
    return defaultValue();
  }

  default T visitFloat(String text) {
    return defaultValue();
  }

  default T visitBoolean(String text) {
    return defaultValue();
  }

  default T visitDate(String text) {
    return defaultValue();
  }

  default T visitNull() {
    return defaultValue();
  }

  default T visitConvertedType(String text) {
    return defaultValue();
  }

  default T defaultValue() {
    return null;
  }
}
