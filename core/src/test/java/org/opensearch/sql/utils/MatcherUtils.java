/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;

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
}
