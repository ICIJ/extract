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

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;

/**
 * A {@linkplain PEMEntry PEM entry} wrapping an <b>un-</b>encrypted
 * {@linkplain KeyPair key pair}.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public final class PEMEntryKeyPair extends PEMEntry<KeyPair> {

    private final KeyPair keyPair;

    /* Restrict construction of instances to this package */
    PEMEntryKeyPair(PEMFactory factory, PrivateKeyInfo key)
    throws NoSuchAlgorithmException, InvalidKeyException, InvalidKeySpecException {
        super(KeyPair.class, false);
        final PrivateKey privateKey = factory.getPrivateKey(key);
        if (privateKey instanceof RSAPrivateCrtKey) {
            final RSAPrivateCrtKey privateCrtKey = (RSAPrivateCrtKey) privateKey;
            final BigInteger modulus = privateCrtKey.getModulus();
            final BigInteger exponent = privateCrtKey.getPublicExponent();
            final RSAPublicKeySpec keySpec = new RSAPublicKeySpec(modulus, exponent);
            final PublicKey publicKey = KeyFactory.getInstance("RSA").generatePublic(keySpec);
            keyPair = new KeyPair(publicKey, privateCrtKey);
        } else {
            throw new InvalidKeyException("Private key " + privateKey.getClass().getName() + " not a RSAPrivateCrtKey");
        }
    }

    /* Restrict construction of instances to this package */
    PEMEntryKeyPair(PEMFactory factory, PEMKeyPair key)
    throws NoSuchAlgorithmException, InvalidKeyException, InvalidKeySpecException {
        super(KeyPair.class, false);
        keyPair = new KeyPair(factory.getPublicKey(key.getPublicKeyInfo()),
                              factory.getPrivateKey(key.getPrivateKeyInfo()));
    }

    @Override
    public KeyPair get() {
        return keyPair;
    }

}
