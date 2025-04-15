/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.LikeQueryIT;

// TODO Like function behaviour in V2 is not correct. Remove when it was fixed in V2.
public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    // TODO: "https://github.com/opensearch-project/sql/issues/3428"
    // disallowCalciteFallback();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_like_with_escaped_percent() throws IOException, IOException {
    super.test_like_with_escaped_percent();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_like_in_where_with_escaped_underscore() throws IOException {
    super.test_like_in_where_with_escaped_underscore();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_like_on_text_field_with_one_word() throws IOException {
    super.test_like_on_text_field_with_one_word();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_like_on_text_keyword_field_with_one_word() throws IOException {
    super.test_like_on_text_keyword_field_with_one_word();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_like_on_text_keyword_field_with_greater_than_one_word() throws IOException {
    super.test_like_on_text_keyword_field_with_greater_than_one_word();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_like_on_text_field_with_greater_than_one_word() throws IOException {
    super.test_like_on_text_field_with_greater_than_one_word();
  }

  @Override
  @Test
  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  public void test_convert_field_text_to_keyword() throws IOException {
    super.test_convert_field_text_to_keyword();
  }
}
