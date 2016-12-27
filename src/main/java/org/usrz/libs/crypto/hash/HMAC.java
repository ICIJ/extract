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
package org.usrz.libs.crypto.hash;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;

import org.usrz.libs.utils.Check;

/**
 * A {@link Function} producing hash-based message authentication codes.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Hash-based_message_authentication_code">Hash-based
 *      message authentication code</a>
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public class HMAC implements Function<HMAC> {

    /* The {@link Hash} wrapped by this instance. */
    private final Hash hash;
    /* The {@link Mac} wrapped by this instance. */
    private final Mac mac;

    /**
     * Create a new {@link HMAC} instance associated with the given {@link Hash}
     * and {@link Mac}.
     */
    protected HMAC(Hash hash, Mac mac) {
        this.hash = Check.notNull(hash, "Null hash");
        this.mac = Check.notNull(mac, "Null mac");
    }

    /* ====================================================================== */

    @Override
    public Hash getHash() {
        return hash;
    }

    /**
     * Return the underlying {@link Mac} associated with this instance.
     */
    public final Mac getMac() {
        return mac;
    }

    /* ====================================================================== */

    @Override
    public final HMAC reset() {
        mac.reset();
        return this;
    }

    @Override
    public HMAC update(byte[] input, int offset, int length) {
        mac.update(input, offset, length);
        return this;
    }

    @Override
    public void finish(byte[] output, int offset) {
        try {
            mac.doFinal(output, offset);
        } catch (ShortBufferException exception) {
            throw new IllegalArgumentException("Buffer too short", exception);
        }
    }

}
