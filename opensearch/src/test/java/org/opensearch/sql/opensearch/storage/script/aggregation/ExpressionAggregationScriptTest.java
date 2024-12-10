/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import com.google.common.collect.ImmutableMap;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.LeafReaderContext;
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
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class ExpressionAggregationScriptTest {

  @Mock private SearchLookup lookup;

  @Mock private LeafSearchLookup leafLookup;

  @Mock private LeafReaderContext context;

  @Test
  void can_execute_expression_with_integer_field() {
    assertThat()
        .docValues("age", 30L) // DocValue only supports long
        .evaluate(DSL.abs(ref("age", INTEGER)))
        .shouldMatch(30);
  }

  @Test
  void can_execute_expression_with_integer_field_with_boolean_result() {
    assertThat()
        .docValues("age", 30L) // DocValue only supports long
        .evaluate(DSL.greater(ref("age", INTEGER), literal(20)))
        .shouldMatch(true);
  }

  @Test
  void can_execute_expression_with_text_keyword_field() {
    assertThat()
        .docValues("name.keyword", "John")
        .evaluate(
            DSL.equal(
                ref(
                    "name",
                    OpenSearchTextType.of(
                        Map.of(
                            "words",
                            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)))),
                literal("John")))
        .shouldMatch(true);
  }

  @Test
  void can_execute_expression_with_null_field() {
    assertThat().docValues("age", null).evaluate(ref("age", INTEGER)).shouldMatch(null);
  }

  @Test
  void can_execute_expression_with_missing_field() {
    assertThat().docValues("age", 30).evaluate(ref("name", STRING)).shouldMatch(null);
  }

  @Test
  void can_execute_parse_expression() {
    assertThat()
        .docValues("age_string", "age: 30")
        .evaluate(
            DSL.regex(
                DSL.ref("age_string", STRING),
                DSL.literal("age: (?<age>\\d+)"),
                DSL.literal("age")))
        .shouldMatch("30");
  }

  @Test
  void can_execute_expression_interpret_dates_for_aggregation() {
    assertThat()
        .docValues("date", "1961-04-12")
        .evaluate(DSL.date(ref("date", STRING)))
        .shouldMatch(new ExprDateValue(LocalDate.of(1961, 4, 12)).timestampValue().toEpochMilli());
  }

  @Test
  void can_execute_expression_interpret_times_for_aggregation() {
    assertThat()
        .docValues("time", "22:13:42")
        .evaluate(DSL.time(ref("time", STRING)))
        .shouldMatch(MILLIS.between(LocalTime.MIN, LocalTime.of(22, 13, 42)));
  }

  @Test
  void can_execute_expression_interpret_timestamps_for_aggregation() {
    assertThat()
        .docValues("timestamp", "1984-03-17 22:16:42")
        .evaluate(DSL.timestamp(ref("timestamp", STRING)))
        .shouldMatch(new ExprTimestampValue("1984-03-17 22:16:42").timestampValue().toEpochMilli());
  }

  @Test
  void can_execute_expression_interpret_non_core_type_for_aggregation() {
    assertThat()
        .docValues("text", "pewpew")
        .evaluate(ref("text", OpenSearchTextType.of()))
        .shouldMatch("pewpew");
  }

  private ExprScriptAssertion assertThat() {
    return new ExprScriptAssertion(lookup, leafLookup, context);
  }

  @RequiredArgsConstructor
  private static class ExprScriptAssertion {
    private final SearchLookup lookup;
    private final LeafSearchLookup leafLookup;
    private final LeafReaderContext context;
    private Object actual;

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

    ExprScriptAssertion evaluate(Expression expr) {
      ExpressionAggregationScript script =
          new ExpressionAggregationScript(expr, lookup, context, Map.of());
      actual = script.execute();
      return this;
    }

    void shouldMatch(Object expected) {
      assertEquals(expected, actual);
    }

    private LeafDocLookup mockLeafDocLookup(Map<String, ScriptDocValues<?>> docValueByNames) {
      LeafDocLookup leafDocLookup = mock(LeafDocLookup.class);
      when(leafDocLookup.get(anyString()))
          .thenAnswer(invocation -> docValueByNames.get(invocation.<String>getArgument(0)));
      return leafDocLookup;
    }
  }

  @RequiredArgsConstructor
  private static class FakeScriptDocValues<T> extends ScriptDocValues<T> {
    private final T value;

    @Override
    public void setNextDocId(int docId) {
      throw new UnsupportedOperationException("Fake script doc values doesn't implement this yet");
    }

    @Override
    public T get(int index) {
      return value;
    }

    @Override
    public int size() {
      return 1;
    }
  }
}
