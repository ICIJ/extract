/* ========================================================================== *
 * Copyright 2014 USRZ.com and Pier Paolo Fumagalli                           *
 * -------------------------------------------------------------------------- *
 * Licensed under the Apache License, Version 2.0 (the "License");            *
 * you may not use this file except in compliance with the License.           *
 * You may obtain a copy of the License at                                    *
 *                                                                            *
 *  http://www.apache.org/licenses/LICENSE-2.0                                *
 *                                                                            *
 * Unless required by applicable law or agreed to in writing, software        *
 * distributed under the License is distributed on an "AS IS" BASIS,          *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 * See the License for the specific language governing permissions and        *
 * limitations under the License.                                             *
 * ========================================================================== */
package org.usrz.libs.crypto.pem;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

/**
 * A {@linkplain PEMEntry PEM entry} wrapping an encrypted
 * {@linkplain KeyPair key pair}.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public final class PEMEntryEncryptedKeyPair extends PEMEntry<KeyPair> {

    private static final JcePEMDecryptorProviderBuilder builder = new JcePEMDecryptorProviderBuilder();

    private final PEMEncryptedKeyPair key;
    private final PEMFactory factory;

    /* Restrict construction of instances to this package */
    PEMEntryEncryptedKeyPair(PEMFactory factory, PEMEncryptedKeyPair key) {
        super(KeyPair.class, true);
        this.factory = factory;
        this.key = key;
    }

    @Override
    public KeyPair get() {
        throw new UnsupportedOperationException("Password for decryption must be supplied");
    }

    @Override
    public KeyPair get(char[] password)
    throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
        final PEMKeyPair key;
        try {
            key = this.key.decryptKeyPair(builder.build(password));
        } catch (IOException exception) {
            throw new InvalidKeyException("Unable to decrypt key pair", exception);
        }

        return new KeyPair(factory.getPublicKey(key.getPublicKeyInfo()),
                           factory.getPrivateKey(key.getPrivateKeyInfo()));
    }

}
