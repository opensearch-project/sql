/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SingleStatementContext;

@ExtendWith(MockitoExtension.class)
public class PPLQueryValidatorTest {
  @Mock GrammarElementValidatorProvider mockedProvider;

  @InjectMocks PPLQueryValidator pplQueryValidator;

  private static final String SOURCE_PREFIX = "source = t | ";

  private enum TestElement {
    FIELDS("fields field1, field1"),
    WHERE("where field1=\"success\""),
    STATS("stats count(), count(`field1`), min(`field1`), max(`field1`)"),
    PARSE("parse `field1` \".*/(?<field2>[^/]+$)\""),
    PATTERNS("patterns new_field='no_numbers' pattern='[0-9]' message"),
    SORT("sort -field1Alias"),
    EVAL("eval field2 = `field` * 2"),
    RENAME("rename field2 as field1"),
    HEAD("head 10"),
    GROK("grok email '.+@%{HOSTNAME:host)'"),
    TOP("top 2 Field1 by Field2"),
    DEDUP("dedup field1"),
    JOIN("join on c_custkey = o_custkey orders"),
    LOOKUP("lookup account_list mkt_id AS mkt_code REPLACE amount, account_name AS name"),
    SUBQUERY("where a > [ source = inner | stats min(c) ]"),
    RARE("rare Field1 by Field2"),
    TRENDLINE("trendline sma(2, field1) as Field1Alias"),
    EVENTSTATS("eventstats sum(field1) by field2"),
    FLATTEN("flatten field1"),
    FIELD_SUMMARY("fieldsummary includefields=field1 nulls=true"),
    FILLNULL("fillnull with 0 in field1"),
    EXPAND("expand employee"),
    DESCRIBE(false, "describe schema.table"),
    STRING_FUNCTIONS("eval cl1Len = LENGTH(col1)"),
    DATETIME_FUNCTIONS("eval newDate = ADDDATE(DATE('2020-08-26'), 1)"),
    CONDITION_FUNCTIONS("eval field2 = isnull(col1)"),
    MATH_FUNCTIONS("eval field2 = ACOS(col1)"),
    EXPRESSIONS("where age > (25 + 5)"),
    IPADDRESS_FUNCTIONS("where cidrmatch(ip, '192.168.0.1/24')"),
    JSON_FUNCTIONS("where cidrmatch(ip, '192.168.0.1/24')"),
    LAMBDA_FUNCTIONS("eval array = json_array(1, -1, 2), result = filter(array, x -> x > 0)"),
    CRYPTO_FUNCTIONS("eval field1 = MD5('hello')");

    @Getter private final String[] queries;

    TestElement(String... queries) {
      this.queries = addPrefix(queries);
    }

    // For describe
    TestElement(boolean addPrefix, String... queries) {
      this.queries = addPrefix ? addPrefix(queries) : queries;
    }

    private String[] addPrefix(String... queries) {
      return Arrays.stream(queries).map(query -> SOURCE_PREFIX + query).toArray(String[]::new);
    }
  }

  @Test
  void testAllowAllByDefault() {
    when(mockedProvider.getValidatorForDatasource(any()))
        .thenReturn(new DefaultGrammarElementValidator());
    VerifyValidator v = new VerifyValidator(pplQueryValidator, DataSourceType.SPARK);
    Arrays.stream(PPLQueryValidatorTest.TestElement.values()).forEach(v::ok);
  }

  @Test
  void testCwlValidator() {
    when(mockedProvider.getValidatorForDatasource(any()))
        .thenReturn(new CWLPPLGrammarElementValidator());
    VerifyValidator v = new VerifyValidator(pplQueryValidator, DataSourceType.SPARK);

    v.ok(TestElement.FIELDS);
    v.ok(TestElement.WHERE);
    v.ok(TestElement.STATS);
    v.ok(TestElement.PARSE);
    v.ng(TestElement.PATTERNS);
    v.ok(TestElement.SORT);
    v.ok(TestElement.EVAL);
    v.ok(TestElement.RENAME);
    v.ok(TestElement.HEAD);
    v.ok(TestElement.GROK);
    v.ok(TestElement.TOP);
    v.ok(TestElement.DEDUP);
    v.ng(TestElement.JOIN);
    v.ng(TestElement.LOOKUP);
    v.ng(TestElement.SUBQUERY);
    v.ok(TestElement.RARE);
    v.ok(TestElement.TRENDLINE);
    v.ok(TestElement.EVENTSTATS);
    v.ng(TestElement.FLATTEN);
    v.ok(TestElement.FIELD_SUMMARY);
    v.ng(TestElement.FILLNULL);
    v.ng(TestElement.EXPAND);
    v.ng(TestElement.DESCRIBE);
    v.ok(TestElement.STRING_FUNCTIONS);
    v.ok(TestElement.DATETIME_FUNCTIONS);
    v.ok(TestElement.CONDITION_FUNCTIONS);
    v.ok(TestElement.MATH_FUNCTIONS);
    v.ok(TestElement.EXPRESSIONS);
    v.ng(TestElement.IPADDRESS_FUNCTIONS);
    v.ng(TestElement.JSON_FUNCTIONS);
    v.ng(TestElement.LAMBDA_FUNCTIONS);
    v.ok(TestElement.CRYPTO_FUNCTIONS);
  }

  @AllArgsConstructor
  private static class VerifyValidator {
    private final PPLQueryValidator validator;
    private final DataSourceType dataSourceType;

    public void ok(PPLQueryValidatorTest.TestElement query) {
      runValidate(query.getQueries());
    }

    public void ng(PPLQueryValidatorTest.TestElement element) {
      Arrays.stream(element.queries)
          .forEach(
              query ->
                  assertThrows(
                      IllegalArgumentException.class,
                      () -> runValidate(query),
                      "The query should throw: query=`" + query.toString() + "`"));
    }

    void runValidate(String[] queries) {
      Arrays.stream(queries).forEach(query -> validator.validate(query, dataSourceType));
    }

    void runValidate(String query) {
      validator.validate(query, dataSourceType);
    }

    SingleStatementContext getParser(String query) {
      org.opensearch.sql.spark.antlr.parser.SqlBaseParser sqlBaseParser =
          new org.opensearch.sql.spark.antlr.parser.SqlBaseParser(
              new CommonTokenStream(
                  new org.opensearch.sql.spark.antlr.parser.SqlBaseLexer(
                      new CaseInsensitiveCharStream(query))));
      return sqlBaseParser.singleStatement();
    }
  }
}
