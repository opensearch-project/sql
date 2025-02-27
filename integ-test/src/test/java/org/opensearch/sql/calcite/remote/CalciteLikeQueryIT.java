/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.LikeQueryIT;

public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  @Override
  @Test
  @Ignore("* in like is handled wrong")
  public void test_like_with_escaped_percent() throws IOException {
    super.test_like_with_escaped_percent();
  }

  @Override
  @Test
  @Ignore("* in like is handled wrong")
  public void test_like_in_where_with_escaped_underscore() throws IOException {
    super.test_like_in_where_with_escaped_underscore();
  }

  @Override
  @Test
  @Ignore("* in like is handled wrong")
  public void test_like_on_text_field_with_one_word() throws IOException {
    super.test_like_on_text_field_with_one_word();
  }

  @Override
  @Test
  @Ignore("* in like is handled wrong")
  public void test_like_on_text_keyword_field_with_one_word() throws IOException {
    super.test_like_on_text_keyword_field_with_one_word();
  }

  @Override
  @Test
  @Ignore("* in like is handled wrong")
  public void test_like_on_text_keyword_field_with_greater_than_one_word() throws IOException {
    super.test_like_on_text_keyword_field_with_greater_than_one_word();
  }

  @Override
  @Test
  @Ignore("* in like is handled wrong")
  public void test_like_on_text_field_with_greater_than_one_word() throws IOException {
    super.test_like_on_text_field_with_greater_than_one_word();
  }

  @Override
  @Test
  @Ignore("ignore this class since IP type is unsupported in calcite engine")
  public void test_convert_field_text_to_keyword() throws IOException {
    super.test_convert_field_text_to_keyword();
  }
}
