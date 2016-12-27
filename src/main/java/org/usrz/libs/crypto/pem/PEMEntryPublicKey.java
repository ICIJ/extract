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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;

/**
 * A {@linkplain PEMEntry PEM entry} wrapping a
 * {@linkplain PublicKey public key}.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public final class PEMEntryPublicKey extends PEMEntry<PublicKey> {

    private final PublicKey key;

    /* Restrict construction of instances to this package */
    PEMEntryPublicKey(PEMFactory factory, SubjectPublicKeyInfo keyInfo)
    throws NoSuchAlgorithmException, InvalidKeyException, InvalidKeySpecException {
        super(PublicKey.class, false);
        key = factory.getPublicKey(keyInfo);

    }

    @Override
    public PublicKey get() {
        return key;
    }

}
