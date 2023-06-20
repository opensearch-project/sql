/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Flint SQL parser that extends Spark SQL parser to parse Flint command first and fall back to
 * Spark parser for unrecognized statement.
 *
 * @param sparkParser
 *   Spark SQL parser
 */
class FlintSparkSqlParser(sparkParser: ParserInterface) extends ParserInterface {

  /**
   * Flint command builder. This has to be lazy because Spark.conf in FlintSpark will create
   * Parser and thus cause stack overflow
   */
  private val flintCmdBuilder = new FlintSparkSqlCommandBuilder()

  override def parsePlan(sqlText: String): LogicalPlan = {
    val flintLexer = new FlintSparkSqlExtensionsLexer(
      new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    flintLexer.removeErrorListeners()
    flintLexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(flintLexer)
    val flintParser = new FlintSparkSqlExtensionsParser(tokenStream)
    flintParser.addParseListener(PostProcessor)
    // parser.addParseListener(UnclosedCommentProcessor(command, tokenStream))
    flintParser.removeErrorListeners()
    flintParser.addErrorListener(ParseErrorListener)

    try {
      val ctx = flintParser.singleStatement()
      flintCmdBuilder.visit(ctx) match {
        case plan: LogicalPlan => plan
        case _ => sparkParser.parsePlan(sqlText)
      }
    } catch {
      case e: ParseException => sparkParser.parsePlan(sqlText)
    }

  }

  /*
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { flintParser =>
    flintCmdBuilder.visit(flintParser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => sparkParser.parsePlan(sqlText)
    }
  }
  */

  override def parseExpression(sqlText: String): Expression = sparkParser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    sparkParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    sparkParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    sparkParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    sparkParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = sparkParser.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = sparkParser.parseQuery(sqlText)

  protected def parse[T](sqlText: String)(toResult: FlintSparkSqlExtensionsParser => T): T = {
    val lexer = new FlintSparkSqlExtensionsLexer(
      new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new FlintSparkSqlExtensionsParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException => throw e
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(
          Option(sqlText),
          e.message,
          position,
          position,
          e.errorClass,
          e.messageParameters)
    }
  }
}

class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName: String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

case object PostProcessor extends FlintSparkSqlExtensionsBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(ctx: ParserRuleContext, stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}
