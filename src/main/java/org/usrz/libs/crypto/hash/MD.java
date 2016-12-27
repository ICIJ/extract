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

import java.security.DigestException;
import java.security.MessageDigest;

import org.usrz.libs.utils.Check;

/**
 * A {@link Function} for simple cryptographic hash function.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Cryptographic_hash_function">Cryptographic
 *      hash function</a>
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public class MD implements Function<MD> {

    /* The {@link Hash} wrapped by this instance. */
    private final Hash hash;
    /* The {@link MessageDigest} wrapped by this instance. */
    private final MessageDigest digest;

    /**
     * Create a new {@link MD} instance associated with the given {@link Hash}
     * and {@link MessageDigest}.
     */
    protected MD(Hash hash, MessageDigest digest) {
        this.hash = Check.notNull(hash, "Null hash");
        this.digest = Check.notNull(digest, "Null message digest");
    }

    /* ====================================================================== */

    @Override
    public Hash getHash() {
        return hash;
    }

    /**
     * Return the underlying {@link MessageDigest} associated with this instance.
     */
    public final MessageDigest getMessageDigest() {
        return digest;
    }

    /* ====================================================================== */

    @Override
    public final MD reset() {
        digest.reset();
        return this;
    }

    @Override
    public MD update(byte[] input, int offset, int length) {
        digest.update(input, offset, length);
        return this;
    }

    @Override
    public void finish(byte[] output, int offset) {
        if (output.length - getHashLength() < offset)
            throw new IllegalArgumentException("Buffer too short");
        try {
            digest.digest(output, offset, getHashLength());
        } catch (DigestException exception) {
            digest.reset();
            throw new IllegalStateException("Digest Exception", exception);
        }
    }

}
