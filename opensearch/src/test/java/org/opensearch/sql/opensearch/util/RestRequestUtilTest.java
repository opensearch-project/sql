package org.opensearch.sql.opensearch.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.rest.RestRequest;

public class RestRequestUtilTest {
  @Test
  public void testConsumeAllRequestParameters() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          RestRequestUtil.consumeAllRequestParameters(null);
        });

    RestRequest request = Mockito.mock(RestRequest.class, Mockito.RETURNS_DEEP_STUBS);

    RestRequestUtil.consumeAllRequestParameters(request);

    Mockito.verify(request.params().keySet(), Mockito.times(1)).forEach(ArgumentMatchers.any());
  }
}
