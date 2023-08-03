/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.encryptor;

public interface Encryptor {

  /**
   * Takes plaintext and returns encrypted text.
   *
   * @param plainText plainText.
   * @return String encryptedText.
   */
  String encrypt(String plainText);

  /**
   * Takes encryptedText and returns plain text.
   *
   * @param encryptedText encryptedText.
   * @return String plainText.
   */
  String decrypt(String encryptedText);

}
