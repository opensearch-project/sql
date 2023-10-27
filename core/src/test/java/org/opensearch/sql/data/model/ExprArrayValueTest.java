package org.opensearch.sql.data.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;

public class ExprArrayValueTest {
  @Test
  public void testIsArrayFalse() {
    assertFalse(new ExprStringValue("test").isArray());
  }
  @Test
  public void testIsArray() {
    ExprArrayValue exprArrayValue = new ExprArrayValue(Arrays.asList(new ExprStringValue("test")));
    assertFalse(exprArrayValue.isArray());
  }

  @Test
  public void testValue() {
    List<ExprValue> value =
        Arrays.asList(new ExprStringValue("test1"), new ExprStringValue("test2"));
    ExprArrayValue exprArrayValue = new ExprArrayValue(value);
    assertEquals(value, exprArrayValue.value());
  }

  @Test
  public void testType() {
    ExprArrayValue exprArrayValue = new ExprArrayValue(Arrays.asList(new ExprStringValue("test")));
    assertEquals(ExprCoreType.ARRAY, exprArrayValue.type());
  }

  @Test
  public void testStringValue() {
    ExprArrayValue exprArrayValue =
        new ExprArrayValue(
            Arrays.asList(new ExprStringValue("test1"), new ExprStringValue("test2")));
    assertEquals("test1,test2", exprArrayValue.stringValue());
  }

  @Test
  public void testArrayValue() {
    List<ExprValue> value =
        Arrays.asList(new ExprStringValue("test1"), new ExprStringValue("test2"));
    ExprArrayValue exprArrayValue = new ExprArrayValue(value);
    assertEquals(value, exprArrayValue.arrayValue());
  }

  @Test
  public void testToString() {
    ExprArrayValue exprArrayValue = new ExprArrayValue(List.of(new ExprStringValue("test")));
    assertEquals("test", exprArrayValue.toString());
  }

  @Test
  public void testCompare() {
    ExprArrayValue exprArrayValue1 = new ExprArrayValue(Arrays.asList(new ExprStringValue("a")));
    ExprArrayValue exprArrayValue2 = new ExprArrayValue(Arrays.asList(new ExprStringValue("b")));
    assertThat(exprArrayValue1.compare(exprArrayValue2), lessThan(0));
  }

  @Test
  public void testEqual() {
    ExprArrayValue exprArrayValue1 = new ExprArrayValue(Arrays.asList(new ExprStringValue("test")));
    ExprArrayValue exprArrayValue2 = new ExprArrayValue(Arrays.asList(new ExprStringValue("test")));
    assertTrue(exprArrayValue1.equal(exprArrayValue2));
  }

  @Test
  public void testHashCode() {
    ExprArrayValue exprArrayValue = new ExprArrayValue(List.of(new ExprStringValue("test")));
    assertEquals(exprArrayValue.hashCode(), Objects.hashCode("test"));
  }
}
