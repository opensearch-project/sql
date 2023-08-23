/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.utils;

import static org.hamcrest.Matchers.equalTo;

import java.util.Locale;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.legacy.utils.StringUtils;

public class StringUtilsTest {

  private Locale originalLocale;

  @Before
  public void saveOriginalLocale() {
    originalLocale = Locale.getDefault();
  }

  @After
  public void restoreOriginalLocale() {
    Locale.setDefault(originalLocale);
  }

  @Test
  public void toLower() {
    final String input = "SAMPLE STRING";
    final String output = StringUtils.toLower(input);

    Assert.assertThat(output, equalTo("sample string"));

    // See
    // https://docs.oracle.com/javase/10/docs/api/java/lang/String.html#toLowerCase(java.util.Locale)
    // for the choice of these characters and the locale.
    final String upper = "\u0130 \u0049";
    Locale.setDefault(Locale.forLanguageTag("tr"));

    Assert.assertThat(upper.toUpperCase(Locale.ROOT), equalTo(StringUtils.toUpper(upper)));
  }

  @Test
  public void toUpper() {
    final String input = "sample string";
    final String output = StringUtils.toUpper(input);

    Assert.assertThat(output, equalTo("SAMPLE STRING"));

    // See
    // https://docs.oracle.com/javase/10/docs/api/java/lang/String.html#toUpperCase(java.util.Locale)
    // for the choice of these characters and the locale.
    final String lower = "\u0069 \u0131";
    Locale.setDefault(Locale.forLanguageTag("tr"));

    Assert.assertThat(lower.toUpperCase(Locale.ROOT), equalTo(StringUtils.toUpper(lower)));
  }

  @Test
  public void format() {
    Locale.setDefault(Locale.forLanguageTag("tr"));
    final String upper = "\u0130 \u0049";
    final String lower = "\u0069 \u0131";

    final String output = StringUtils.format("%s %s", upper, lower);
    Assert.assertThat(output, equalTo(String.format(Locale.ROOT, "%s %s", upper, lower)));
  }
}
