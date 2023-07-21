/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.encryptor;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.crypto.spec.SecretKeySpec;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor
public class EncryptorImpl implements Encryptor {

  private final String masterKey;

  @Override
  public String encrypt(String plainText) {
    validate(masterKey);
    final AwsCrypto crypto = AwsCrypto.builder()
        .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
        .build();

    JceMasterKey jceMasterKey
        = JceMasterKey.getInstance(new SecretKeySpec(masterKey.getBytes(), "AES"), "Custom",
        "opensearch.config.master.key", "AES/GCM/NoPadding");

    final CryptoResult<byte[], JceMasterKey> encryptResult = crypto.encryptData(jceMasterKey,
        plainText.getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(encryptResult.getResult());
  }

  @Override
  public String decrypt(String encryptedText) {
    validate(masterKey);
    final AwsCrypto crypto = AwsCrypto.builder()
        .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
        .build();

    JceMasterKey jceMasterKey
        = JceMasterKey.getInstance(new SecretKeySpec(masterKey.getBytes(), "AES"), "Custom",
        "opensearch.config.master.key", "AES/GCM/NoPadding");

    final CryptoResult<byte[], JceMasterKey> decryptedResult
        = crypto.decryptData(jceMasterKey, Base64.getDecoder().decode(encryptedText));
    return new String(decryptedResult.getResult());
  }

  private void validate(String masterKey) {
    if (StringUtils.isEmpty(masterKey)) {
      throw new IllegalStateException(
          "Master key is a required config for using create and update datasource APIs."
              + "Please set plugins.query.datasources.encryption.masterkey config "
              + "in opensearch.yml in all the cluster nodes. "
              + "More details can be found here: "
              + "https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/"
              + "admin/datasources.rst#master-key-config-for-encrypting-credential-information");
    }
  }


}
