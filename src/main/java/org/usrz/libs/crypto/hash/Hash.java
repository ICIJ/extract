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

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * An enumeration of all supported hashing algorithms.
 * <p>
 * The standard JSSE provided {@link MessageDigest} and {@link Mac}
 * instantiation is extremely costly and (in case of {@link Mac}s) does not
 * support initialization with empty keys.
 * <p>
 * This enumeration offers a convenient way to instantiate {@link Mac}s and
 * {@link MessageDigest}s associated with well known hashing algorithms,
 * relying on cloning instances and pooling generated instances using a
 * {@link ThreadLocal}, and support empty keys for {@link Mac}s.
 * <p>
 * <b>NOTE:</b> The current JSSE included with Java 7 does not support the SHA2
 * algorithm with 224-bits hash values, this feature will be added in
 * <a href="https://blogs.oracle.com/mullan/entry/jep_130_sha_224_message">Java 8</a>.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public enum Hash {

    /**
     * The <a href="http://en.wikipedia.org/wiki/MD5">MD5</a> hashing
     * algorithm producing <b>128-bits</b> hash values.
     */
    MD5 ("MD5", "HmacMD5"),

    /**
     * The <a href="http://en.wikipedia.org/wiki/SHA-1">SHA-1</a> hashing
     * algorithm producing <b>160-bits</b> hash values.
     */
    SHA1 ("SHA1", "HmacSHA1"),

    /**
     * The <a href="http://en.wikipedia.org/wiki/SHA-2">SHA-2</a> hashing
     * algorithm producing <b>256-bits</b> hash values.
     */
    SHA256 ("SHA-256", "HmacSHA256"),

    /**
     * The <a href="http://en.wikipedia.org/wiki/SHA-2">SHA-2</a> hashing
     * algorithm producing <b>384-bits</b> hash values.
     */
    SHA384 ("SHA-384", "HmacSHA384"),

    /**
     * The <a href="http://en.wikipedia.org/wiki/SHA-2">SHA-2</a> algorithm
     * algorithm producing <b>512-bits</b> hash values.
     */
    SHA512 ("SHA-512", "HmacSHA512");

    /* ====================================================================== */

    /* An empty secret key for Mac initialization */
    private final SecretKeySpec emptySecretKeySpec;

    /* A shared Mac instance from which to clone from. */
    private final Mac sharedMac;

    /* A shared MessageDigest instance from which to clone from. */
    private final MessageDigest sharedDigest;

    /* The length of hashes produced by this algorithm. */
    private final int length;

    /* Keep copies of Mac instances around in a ThreadLocal. */
    private final ThreadLocal<Mac> macPool = new ThreadLocal<Mac>() {
        @Override
        public Mac initialValue() {
            try {
                return (Mac) sharedMac.clone();
            } catch (CloneNotSupportedException exception) {
                final String name = sharedMac.getClass().getName();
                final Error error = new InternalError(name + " not cloneable");
                throw (Error) error.initCause(exception);
            }
        }
    };

    /* Keep copies of MessageDigest instances around in a ThreadLocal. */
    private final ThreadLocal<MessageDigest> digestPool = new ThreadLocal<MessageDigest>() {
        @Override
        public MessageDigest initialValue() {
            try {
                return (MessageDigest) sharedDigest.clone();
            } catch (CloneNotSupportedException exception) {
                final String name = sharedDigest.getClass().getName();
                final Error error = new InternalError(name + " not cloneable");
                throw (Error) error.initCause(exception);
            }
        }
    };

    /* ====================================================================== */

    /* Create a new PBKDF2Algorithm instance with the given algorithm name. */
    private Hash(final String digestAlgorithm, String macAlgorithm) {

        /* Create a shared Mac instance */
        try {
            sharedDigest = MessageDigest.getInstance(digestAlgorithm);
            sharedMac = Mac.getInstance(macAlgorithm);
            length = sharedDigest.getDigestLength();
        } catch (NoSuchAlgorithmException exception) {
            final Error error = new InternalError("Invalid algorithm " + macAlgorithm);
            throw (Error) error.initCause(exception);
        }

        /* Create an empty secret key spec instance for this algorithm */
        emptySecretKeySpec = new SecretKeySpec("foo".getBytes(), sharedMac.getAlgorithm()) {

            private final byte[] empty = new byte[0];

            @Override
            public byte[] getEncoded() {
                return empty;
            }
        };

        /* Check sizes of digest and mac */
        if (sharedDigest.getDigestLength() != sharedMac.getMacLength()) {
            final String digestClass = sharedDigest.getClass().getName();
            final String macClass = sharedMac.getClass().getName();
            throw new InternalError("Length mismatch between " + digestClass + "["
                        + digestAlgorithm + "] (" + sharedDigest.getDigestLength()
                        + ") and " + macClass + "[" + macAlgorithm + "] ("
                        + sharedMac.getMacLength() + ")");
        }

        /* Chech proper functionality */
        digest().finish();
        hmac(null).finish();
    }

    /* ====================================================================== */

    /**
     * Return the length (in bytes) of the digest or mac produced by this.
     */
    public int getHashLength() {
        return length;
    }

    /**
     * Return an {@link MD} {@linkplain Function function} digesting data
     * with this {@link Hash}.
     */
    public MD digest() {
        return new MD(this, getMessageDigest());
    }

    /**
     * Return an {@link HMAC} {@linkplain Function function} initialized
     * with the specified key digesting data with this {@link Hash}.
     * <p>
     * If the specified key is empty or <b>null</b> the {@link HMAC} will be
     * initialized with an empty key.
     */
    public HMAC hmac(byte[] key) {
        return new HMAC(this, getMac(key));
    }

    /* ====================================================================== */

    /* Return an instance of a {@link MessageDigest}. */
    private MessageDigest getMessageDigest() {
        final MessageDigest digest = digestPool.get();
        digest.reset();
        return digest;
    }

    /* Return a {@link Mac} instance initialized with the specified key */
    private Mac getMac(byte[] key) {
        final Mac mac = macPool.get();
        try {
            mac.init(key == null ? emptySecretKeySpec :
                     key.length == 0 ?  emptySecretKeySpec :
                     new SecretKeySpec(key, mac.getAlgorithm()));
            return mac;
        } catch (InvalidKeyException exception) {
            throw new IllegalArgumentException("Invalid key", exception);
        }
    }

}