package org.opensearch.sql.common.utils;

import inet.ipaddr.IPAddressString;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/** Utility Class for URI host validation. */
public class URIValidationUtils {

  public static boolean validateURIHost(String host, List<String> denyHostList)
      throws UnknownHostException {
    IPAddressString ipStr = new IPAddressString(InetAddress.getByName(host).getHostAddress());
    for (String denyHost : denyHostList) {
      IPAddressString denyHostStr = new IPAddressString(denyHost);
      if (denyHostStr.contains(ipStr)) {
        return false;
      }
    }
    return true;
  }
}
