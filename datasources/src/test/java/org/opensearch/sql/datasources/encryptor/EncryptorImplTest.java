/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.encryptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class EncryptorImplTest {

  @Test
  public void testEncryptAndDecrypt() {
    String masterKey = "1234567890123456";
    String input = "This is a test input";
    Encryptor encryptor = new EncryptorImpl(masterKey);

    String encrypted = encryptor.encrypt(input);
    String decrypted = encryptor.decrypt(encrypted);

    assertEquals(input, decrypted);
  }

  @Test
  public void testMasterKeySize() {
    String input = "This is a test input";
    String masterKey8 = "12345678";
    Encryptor encryptor8 = new EncryptorImpl(masterKey8);
    assertThrows(AwsCryptoException.class, () -> {
      encryptor8.encrypt(input);
    });

    String masterKey16 = "1234567812345678";
    Encryptor encryptor16 = new EncryptorImpl(masterKey16);
    String encrypted = encryptor16.encrypt(input);
    Assertions.assertEquals(input, encryptor16.decrypt(encrypted));

    String masterKey24 = "123456781234567812345678";
    Encryptor encryptor24 = new EncryptorImpl(masterKey24);
    encrypted = encryptor24.encrypt(input);
    Assertions.assertEquals(input, encryptor24.decrypt(encrypted));

    String masterKey17 = "12345678123456781";
    Encryptor encryptor17 = new EncryptorImpl(masterKey17);
    assertThrows(AwsCryptoException.class, () -> {
      encryptor17.encrypt(input);
    });
  }

  @Test
  public void testInvalidBase64String() {
    String encrypted = "invalidBase64String";
    Encryptor encryptor = new EncryptorImpl("randomMasterKey");

    assertThrows(BadCiphertextException.class, () -> {
      encryptor.decrypt(encrypted);
    });
  }

  @Test
  public void testDecryptWithDifferentKey() {

    String masterKeyOne = "1234567890123456";
    String masterKeyTwo = "1234567890123455";
    String input = "This is a test input";
    Encryptor encryptor1 = new EncryptorImpl(masterKeyOne);
    Encryptor encryptor2 = new EncryptorImpl(masterKeyTwo);

    String encrypted = encryptor1.encrypt(input);

    assertThrows(Exception.class, () -> {
      encryptor2.decrypt(encrypted);
    });
  }

  @Test
  public void testEncryptionAndDecryptionWithNullMasterKey() {
    String input = "This is a test input";
    Encryptor encryptor = new EncryptorImpl(null);
    IllegalStateException illegalStateException
        = Assertions.assertThrows(IllegalStateException.class,
              () -> encryptor.encrypt(input));
    Assertions.assertEquals("Master key is a required config for using create and"
            + " update datasource APIs."
            + "Please set plugins.query.datasources.encryption.masterkey config "
            + "in opensearch.yml in all the cluster nodes. "
            + "More details can be found here: "
            + "https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/"
            + "admin/datasources.rst#master-key-config-for-encrypting-credential-information",
        illegalStateException.getMessage());
    illegalStateException
        = Assertions.assertThrows(IllegalStateException.class,
              () -> encryptor.decrypt(input));
    Assertions.assertEquals("Master key is a required config for using create and"
            + " update datasource APIs."
            + "Please set plugins.query.datasources.encryption.masterkey config "
            + "in opensearch.yml in all the cluster nodes. "
            + "More details can be found here: "
            + "https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/"
            + "admin/datasources.rst#master-key-config-for-encrypting-credential-information",
        illegalStateException.getMessage());
  }

  @Test
  public void testEncryptionAndDecryptionWithEmptyMasterKey() {
    String masterKey = "";
    String input = "This is a test input";
    Encryptor encryptor = new EncryptorImpl(masterKey);
    IllegalStateException illegalStateException
        = Assertions.assertThrows(IllegalStateException.class,
              () -> encryptor.encrypt(input));
    Assertions.assertEquals("Master key is a required config for using create and"
            + " update datasource APIs."
            + "Please set plugins.query.datasources.encryption.masterkey config "
            + "in opensearch.yml in all the cluster nodes. "
            + "More details can be found here: "
            + "https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/"
            + "admin/datasources.rst#master-key-config-for-encrypting-credential-information",
        illegalStateException.getMessage());
    illegalStateException
        = Assertions.assertThrows(IllegalStateException.class,
              () -> encryptor.decrypt(input));
    Assertions.assertEquals("Master key is a required config for using create and"
            + " update datasource APIs."
            + "Please set plugins.query.datasources.encryption.masterkey config "
            + "in opensearch.yml in all the cluster nodes. "
            + "More details can be found here: "
            + "https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/"
            + "admin/datasources.rst#master-key-config-for-encrypting-credential-information",
        illegalStateException.getMessage());
  }

}
