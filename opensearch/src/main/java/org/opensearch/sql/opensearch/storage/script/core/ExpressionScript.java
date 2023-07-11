/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.core;

import static java.util.stream.Collectors.toMap;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.chrono.ChronoZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.EqualsAndHashCode;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * Expression script executor that executes the expression on each document
 * and determine if the document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
public class ExpressionScript {

  /**
   * Expression to execute.
   */
  private final Expression expression;

  /**
   * ElasticsearchExprValueFactory.
   */
  @EqualsAndHashCode.Exclude
  private final OpenSearchExprValueFactory valueFactory;

  /**
   * Reference Fields.
   */
  @EqualsAndHashCode.Exclude
  private final Set<ReferenceExpression> fields;

  /**
   * Expression constructor.
   */
  public ExpressionScript(Expression expression) {
    this.expression = expression;
    this.fields = AccessController.doPrivileged((PrivilegedAction<Set<ReferenceExpression>>) () ->
        extractFields(expression));
    this.valueFactory =
        AccessController.doPrivileged(
            (PrivilegedAction<OpenSearchExprValueFactory>) () -> buildValueFactory(fields));
  }

  /**
   * Evaluate on the doc generate by the doc provider.
   *
   * @param docProvider doc provider.
   * @param evaluator   evaluator
   * @return expr value
   */
  public ExprValue execute(Supplier<Map<String, ScriptDocValues<?>>> docProvider,
                           BiFunction<Expression,
                               Environment<Expression,
                                   ExprValue>, ExprValue> evaluator) {
    return AccessController.doPrivileged((PrivilegedAction<ExprValue>) () -> {
      Environment<Expression, ExprValue> valueEnv =
          buildValueEnv(fields, valueFactory, docProvider);
      ExprValue result = evaluator.apply(expression, valueEnv);
      return result;
    });
  }

  private Set<ReferenceExpression> extractFields(Expression expr) {
    Set<ReferenceExpression> fields = new HashSet<>();
    expr.accept(new ExpressionNodeVisitor<Object, Set<ReferenceExpression>>() {
      @Override
      public Object visitReference(ReferenceExpression node, Set<ReferenceExpression> context) {
        context.add(node);
        return null;
      }

      @Override
      public Object visitParse(ParseExpression node, Set<ReferenceExpression> context) {
        node.getSourceField().accept(this, context);
        return null;
      }
    }, fields);
    return fields;
  }

  private OpenSearchExprValueFactory buildValueFactory(Set<ReferenceExpression> fields) {
    Map<String, OpenSearchDataType> typeEnv = fields.stream().collect(toMap(
        ReferenceExpression::getAttr, e -> OpenSearchDataType.of(e.type())));
    return new OpenSearchExprValueFactory(typeEnv);
  }

  private Environment<Expression, ExprValue> buildValueEnv(
      Set<ReferenceExpression> fields, OpenSearchExprValueFactory valueFactory,
      Supplier<Map<String, ScriptDocValues<?>>> docProvider) {

    Map<Expression, ExprValue> valueEnv = new HashMap<>();
    for (ReferenceExpression field : fields) {
      String fieldName = field.getAttr();
      ExprValue exprValue = valueFactory.construct(
          fieldName,
          getDocValue(field, docProvider),
          false
      );
      valueEnv.put(field, exprValue);
    }
    // Encapsulate map data structure into anonymous Environment class
    return valueEnv::get;
  }

  private Object getDocValue(ReferenceExpression field,
                             Supplier<Map<String, ScriptDocValues<?>>> docProvider) {
    String fieldName = OpenSearchTextType.convertTextToKeyword(field.getAttr(), field.type());
    ScriptDocValues<?> docValue = docProvider.get().get(fieldName);
    if (docValue == null || docValue.isEmpty()) {
      return null; // No way to differentiate null and missing from doc value
    }

    Object value = docValue.get(0);
    if (value instanceof ChronoZonedDateTime) {
      return ((ChronoZonedDateTime<?>) value).toInstant();
    }
    return castNumberToFieldType(value, field.type());
  }

  /**
   * DocValue only support long and double so cast to integer and float if needed.
   * The doc value must be Long and Double for expr type Long/Integer and Double/Float respectively.
   * Otherwise there must be bugs in our engine that causes the mismatch.
   */
  private Object castNumberToFieldType(Object value, ExprType type) {
    if (value == null) {
      return value;
    }

    if (type == INTEGER) {
      return ((Long) value).intValue();
    } else if (type == FLOAT) {
      return ((Double) value).floatValue();
    } else {
      return value;
    }
  }
}
