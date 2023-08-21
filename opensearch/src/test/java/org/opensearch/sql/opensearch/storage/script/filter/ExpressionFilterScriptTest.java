/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import com.google.common.collect.ImmutableMap;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.LeafReaderContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.search.lookup.LeafDocLookup;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class ExpressionFilterScriptTest {

  @Mock private SearchLookup lookup;

  @Mock private LeafSearchLookup leafLookup;

  @Mock private LeafReaderContext context;

  @Test
  void should_match_if_true_literal() {
    assertThat().docValues().filterBy(literal(true)).shouldMatch();
  }

  @Test
  void should_not_match_if_false_literal() {
    assertThat().docValues().filterBy(literal(false)).shouldNotMatch();
  }

  @Test
  void can_execute_expression_with_integer_field() {
    assertThat()
        .docValues("age", 30L) // DocValue only supports long
        .filterBy(DSL.greater(ref("age", INTEGER), literal(20)))
        .shouldMatch();
  }

  @Test
  void can_execute_expression_with_text_keyword_field() {
    assertThat()
        .docValues("name.keyword", "John")
        .filterBy(
            DSL.equal(
                ref(
                    "name",
                    OpenSearchTextType.of(
                        Map.of(
                            "words",
                            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)))),
                literal("John")))
        .shouldMatch();
  }

  @Test
  void can_execute_expression_with_float_field() {
    assertThat()
        .docValues(
            "balance", 100.0, // DocValue only supports double
            "name", "John")
        .filterBy(
            DSL.and(
                DSL.less(ref("balance", FLOAT), literal(150.0F)),
                DSL.equal(ref("name", STRING), literal("John"))))
        .shouldMatch();
  }

  @Test
  void can_execute_expression_with_timestamp_field() {
    ExprTimestampValue ts = new ExprTimestampValue("2020-08-04 10:00:00");
    assertThat()
        .docValues("birthday", ZonedDateTime.parse("2020-08-04T10:00:00Z"))
        .filterBy(DSL.equal(ref("birthday", TIMESTAMP), new LiteralExpression(ts)))
        .shouldMatch();
  }

  @Test
  void can_execute_expression_with_date_field() {
    ExprDateValue date = new ExprDateValue("2020-08-04");
    assertThat()
        .docValues("birthday", "2020-08-04")
        .filterBy(DSL.equal(ref("birthday", DATE), new LiteralExpression(date)))
        .shouldMatch();
  }

  @Test
  void can_execute_expression_with_time_field() {
    ExprTimeValue time = new ExprTimeValue("10:00:01");
    assertThat()
        .docValues("birthday", "10:00:01")
        .filterBy(DSL.equal(ref("birthday", TIME), new LiteralExpression(time)))
        .shouldMatch();
  }

  @Test
  void can_execute_expression_with_missing_field() {
    assertThat().docValues("age", 30).filterBy(ref("name", STRING)).shouldNotMatch();
  }

  @Test
  void can_execute_expression_with_empty_doc_value() {
    assertThat().docValues("name", emptyList()).filterBy(ref("name", STRING)).shouldNotMatch();
  }

  @Test
  void can_execute_parse_expression() {
    assertThat()
        .docValues("age_string", "age: 30")
        .filterBy(
            DSL.equal(
                DSL.regex(
                    DSL.ref("age_string", STRING), literal("age: (?<age>\\d+)"), literal("age")),
                literal("30")))
        .shouldMatch();
  }

  @Test
  void cannot_execute_non_predicate_expression() {
    assertThrow(
            IllegalStateException.class,
            "Expression has wrong result type instead of boolean: expression [10], result [10]")
        .docValues()
        .filterBy(literal(10));
  }

  private ExprScriptAssertion assertThat() {
    return new ExprScriptAssertion(lookup, leafLookup, context);
  }

  private <T extends Throwable> ExprScriptAssertion assertThrow(Class<T> clazz, String message) {
    return new ExprScriptAssertion(lookup, leafLookup, context) {
      @Override
      ExprScriptAssertion filterBy(Expression expr) {
        Throwable t = assertThrows(clazz, () -> super.filterBy(expr));
        assertEquals(message, t.getMessage());
        return null;
      }
    };
  }

  @RequiredArgsConstructor
  private static class ExprScriptAssertion {
    private final SearchLookup lookup;
    private final LeafSearchLookup leafLookup;
    private final LeafReaderContext context;
    private boolean isMatched;

    ExprScriptAssertion docValues() {
      return this;
    }

    ExprScriptAssertion docValues(String name, Object value) {
      LeafDocLookup leafDocLookup =
          mockLeafDocLookup(ImmutableMap.of(name, new FakeScriptDocValues<>(value)));

      when(lookup.getLeafSearchLookup(any())).thenReturn(leafLookup);
      when(leafLookup.doc()).thenReturn(leafDocLookup);
      return this;
    }

    ExprScriptAssertion docValues(String name1, Object value1, String name2, Object value2) {
      LeafDocLookup leafDocLookup =
          mockLeafDocLookup(
              ImmutableMap.of(
                  name1, new FakeScriptDocValues<>(value1),
                  name2, new FakeScriptDocValues<>(value2)));

      when(lookup.getLeafSearchLookup(any())).thenReturn(leafLookup);
      when(leafLookup.doc()).thenReturn(leafDocLookup);
      return this;
    }

    ExprScriptAssertion filterBy(Expression expr) {
      ExpressionFilterScript script = new ExpressionFilterScript(expr, lookup, context, emptyMap());
      isMatched = script.execute();
      return this;
    }

    void shouldMatch() {
      Assertions.assertTrue(isMatched);
    }

    void shouldNotMatch() {
      Assertions.assertFalse(isMatched);
    }

    private LeafDocLookup mockLeafDocLookup(Map<String, ScriptDocValues<?>> docValueByNames) {
      LeafDocLookup leafDocLookup = mock(LeafDocLookup.class);
      when(leafDocLookup.get(anyString()))
          .thenAnswer(invocation -> docValueByNames.get(invocation.<String>getArgument(0)));
      return leafDocLookup;
    }
  }

  private static class FakeScriptDocValues<T> extends ScriptDocValues<T> {
    private final List<T> values;

    @SuppressWarnings("unchecked")
    public FakeScriptDocValues(T value) {
      this.values = (value instanceof List) ? (List<T>) value : singletonList(value);
    }

    @Override
    public void setNextDocId(int docId) {
      throw new UnsupportedOperationException("Fake script doc values doesn't implement this yet");
    }

    @Override
    public T get(int index) {
      return values.get(index);
    }

    @Override
    public int size() {
      return values.size();
    }
  }
}
