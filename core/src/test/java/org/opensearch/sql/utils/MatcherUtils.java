/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;

import java.util.Objects;

/** Matcher Utils. */
public class MatcherUtils {
  /** Check {@link ExprValue} type equal to {@link ExprCoreType}. */
  public static TypeSafeMatcher<ExprValue> hasType(ExprCoreType type) {
    return new TypeSafeMatcher<ExprValue>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(type.toString());
      }

      @Override
      protected boolean matchesSafely(ExprValue value) {
        return type == value.type();
      }
    };
  }

  /** Check {@link ExprValue} value equal to {@link Object}. */
  public static TypeSafeMatcher<ExprValue> hasValue(Object object) {
    return new TypeSafeMatcher<ExprValue>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(object.toString());
      }

      @Override
      protected boolean matchesSafely(ExprValue value) {
        return object.equals(value.value());
      }
    };
  }

  public static TypeSafeDiagnosingMatcher<ExprValue> containsValue(String key, Object value) {
    Objects.requireNonNull(key, "Key is required");
    Objects.requireNonNull(value, "Value is required");
    return new TypeSafeDiagnosingMatcher<>() {

      @Override
      protected boolean matchesSafely(ExprValue item, Description mismatchDescription) {
        ExprValue expressionForKey = item.keyValue(key);
        if(Objects.isNull(expressionForKey)) {
          mismatchDescription.appendValue(item).appendText(" does not contain key ").appendValue(key);
          return false;
        }
        Object givenValue = expressionForKey.value();
        if(value.equals(givenValue)) {
          return true;
        }
        mismatchDescription.appendText(" value for key ").appendValue(key).appendText(" was ").appendValue(givenValue);
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("ExprValue should contain key ").appendValue(key).appendText(" with string value ").appendValue(value);
      }
    };
  }

  public static TypeSafeDiagnosingMatcher<ExprValue> containsNull(String key) {
    Objects.requireNonNull(key, "Key is required");
    return new TypeSafeDiagnosingMatcher<>() {

      @Override
      protected boolean matchesSafely(ExprValue item, Description mismatchDescription) {
        ExprValue expressionForKey = item.keyValue(key);
        if(Objects.isNull(expressionForKey)) {
          mismatchDescription.appendValue(item).appendText(" does not contain key ").appendValue(key);
          return false;
        }
        if(expressionForKey.isNull()) {
          return true;
        }
        mismatchDescription.appendText(" value for key ").appendValue(key).appendText(" was ").appendValue(expressionForKey.value());
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("ExprValue should contain key ").appendValue(key).appendText(" with null value ");
      }
    };
  }
}
