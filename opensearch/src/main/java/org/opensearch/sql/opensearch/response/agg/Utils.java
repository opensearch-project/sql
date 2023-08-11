/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.opensearch.response.agg;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {
  /**
   * Utils to handle Nan/Infinite Value.
   *
   * @return null if is Nan or is +-Infinity.
   */
  public static Object handleNanInfValue(double value) {
    return Double.isNaN(value) || Double.isInfinite(value) ? null : value;
  }
}
