/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.storage

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Todo. find the right package.
 */
case class FlintQueryCompiler(schema: StructType) {

  /**
   * Using AND to concat predicates. Todo. If spark spark.sql.ansi.enabled = true, more expression
   * defined in V2ExpressionBuilder could be pushed down.
   */
  def compile(predicates: Array[Predicate]): String = {
    if (predicates.isEmpty) {
      return ""
    }
    compile(predicates.reduce(new And(_, _)))
  }

  /**
   * Compile Expression to Flint query string.
   *
   * @param expr
   *   Expression.
   * @return
   *   empty if does not support.
   */
  def compile(expr: Expression, quoteString: Boolean = true): String = {
    expr match {
      case LiteralValue(value, dataType) =>
        if (quoteString && dataType == StringType) {
          s""""${Literal(value, dataType).toString()}""""
        } else {
          Literal(value, dataType).toString()
        }
      case p: Predicate => visitPredicate(p)
      case f: FieldReference => f.toString()
      case _ => ""
    }
  }

  /**
   * Predicate is defined in SPARK filters.scala. Todo.
   *   1. currently, we map spark contains to OpenSearch match query. Can we leverage more full
   *      text queries for text field. 2. configuration of expensive query.
   */
  def visitPredicate(p: Predicate): String = {
    val name = p.name()
    name match {
      case "IS_NULL" =>
        s"""{"bool":{"must_not":{"exists":{"field":"${compile(p.children()(0))}"}}}}"""
      case "IS_NOT_NULL" =>
        s"""{"exists":{"field":"${compile(p.children()(0))}"}}"""
      case "AND" =>
        s"""{"bool":{"filter":[${compile(p.children()(0))},${compile(p.children()(1))}]}}"""
      case "OR" =>
        s"""{"bool":{"should":[{"bool":{"filter":${compile(
            p.children()(0))}}},{"bool":{"filter":${compile(p.children()(1))}}}]}}"""
      case "NOT" =>
        s"""{"bool":{"must_not":${compile(p.children()(0))}}}"""
      case "=" =>
        s"""{"term":{"${compile(p.children()(0))}":{"value":${compile(p.children()(1))}}}}"""
      case ">" =>
        s"""{"range":{"${compile(p.children()(0))}":{"gt":${compile(p.children()(1))}}}}"""
      case ">=" =>
        s"""{"range":{"${compile(p.children()(0))}":{"gte":${compile(p.children()(1))}}}}"""
      case "<" =>
        s"""{"range":{"${compile(p.children()(0))}":{"lt":${compile(p.children()(1))}}}}"""
      case "<=" =>
        s"""{"range":{"${compile(p.children()(0))}":{"lte":${compile(p.children()(1))}}}}"""
      case "IN" =>
        val values = p.children().tail.map(expr => compile(expr)).mkString("[", ",", "]")
        s"""{"terms":{"${compile(p.children()(0))}":$values}}"""
      case "STARTS_WITH" =>
        s"""{"prefix":{"${compile(p.children()(0))}":{"value":${compile(p.children()(1))}}}}"""
      case "CONTAINS" =>
        val fieldName = compile(p.children()(0))
        if (isTextField(fieldName)) {
          s"""{"match":{"$fieldName":{"query":${compile(p.children()(1))}}}}"""
        } else {
          s"""{"wildcard":{"$fieldName":{"value":"*${compile(p.children()(1), false)}*"}}}"""
        }
      case "ENDS_WITH" =>
        s"""{"wildcard":{"${compile(p.children()(0))}":{"value":"*${compile(
            p.children()(1),
            false)}"}}}"""
      case _ => ""
    }
  }

  /**
   * return true if the field is Flint Text field.
   */
  protected def isTextField(attribute: String): Boolean = {
    schema.apply(attribute) match {
      case StructField(_, StringType, _, metadata) =>
        metadata.contains("osType") && metadata.getString("osType") == "text"
      case _ => false
    }
  }
}
