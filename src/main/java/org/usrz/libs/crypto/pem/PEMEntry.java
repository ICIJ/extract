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
import java.security.spec.InvalidKeySpecException;


/**
 * An abstract class representing an entry in a <i>PEM-encoded</i> file.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 * @param <T> The Java type of the object contained in this entry.
 */
public abstract class PEMEntry<T> {

    private final boolean encrypted;
    private final Class<T> type;

    /* Restrict construction of instances to this package */
    PEMEntry(Class<T> type, boolean encrypted) {
        this.encrypted = encrypted;
        this.type = type;
    }

    /* ====================================================================== */

    /**
     * Return the type of objects held by this {@linkplain PEMEntry entry}.
     */
    public final Class<T> getType() {
        return type;
    }

    /**
     * Checks whether this {@linkplain PEMEntry entry} is encrypted or not.
     */
    public final boolean isEncrypted() {
        return this.encrypted;
    }

    /**
     * Return the value of this unencrypted {@linkplain PEMEntry entry} as a
     * Java object.
     */
    public abstract T get();

    /**
     * Return the value of this encrypyrf {@linkplain PEMEntry entry} as a
     * Java object.
     */
    public T get(char[] password)
    throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
        return this.get();
    }

}
