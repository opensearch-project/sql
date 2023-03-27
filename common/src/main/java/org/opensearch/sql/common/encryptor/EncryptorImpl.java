/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.common.encryptor;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.crypto.spec.SecretKeySpec;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EncryptorImpl implements Encryptor {

  private final String masterKey;

  @Override
  public String encrypt(String plainText) {

    final AwsCrypto crypto = AwsCrypto.builder()
        .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
        .build();

    JceMasterKey jceMasterKey
        = JceMasterKey.getInstance(new SecretKeySpec(masterKey.getBytes(), "AES"), "Custom", "",
        "AES/GCM/NoPadding");

    final CryptoResult<byte[], JceMasterKey> encryptResult = crypto.encryptData(jceMasterKey,
        plainText.getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(encryptResult.getResult());
  }

  @Override
  public String decrypt(String encryptedText) {
    final AwsCrypto crypto = AwsCrypto.builder()
        .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
        .build();

    JceMasterKey jceMasterKey
        = JceMasterKey.getInstance(new SecretKeySpec(masterKey.getBytes(), "AES"), "Custom", "",
        "AES/GCM/NoPadding");

    final CryptoResult<byte[], JceMasterKey> decryptedResult
        = crypto.decryptData(jceMasterKey, Base64.getDecoder().decode(encryptedText));
    return new String(decryptedResult.getResult());
  }

}