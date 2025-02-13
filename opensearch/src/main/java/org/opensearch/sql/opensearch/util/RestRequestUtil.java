package org.opensearch.sql.opensearch.util;

import lombok.NonNull;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

/** RestRequestUtil is a utility class for common operations on OpenSearch RestRequest's. */
public class RestRequestUtil {

  private RestRequestUtil() {
    // utility class
  }

  /**
   * Utility method for consuming all the request parameters. Doing this will ensure that the
   * BaseRestHandler doesn't fail the request with an unconsumed parameter exception.
   *
   * @see org.opensearch.rest.BaseRestHandler#handleRequest(RestRequest, RestChannel, NodeClient)
   * @param request - The request to consume all parameters on
   */
  public static void consumeAllRequestParameters(@NonNull RestRequest request) {
    request.params().keySet().forEach(request::param);
  }
}
