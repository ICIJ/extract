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

/**
 * A component capable of producing a hash for a <code>byte[]</code>.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 * @param <F> The concrete type of {@link Function} implemented.
 */
public interface Function<F extends Function<F>> {

    /**
     * Return the {@link Hash} associated with this instance.
     */
    public Hash getHash();

    /**
     * Return the length, in bytes, of the hashes produced  by this instance.
     */
    default int getHashLength() {
        return getHash().getHashLength();
    }

    /**
     * Updates the hash value using the specified byte.
     */
    public default F update(byte input) {
        return update(new byte[] { input }, 0, 1);
    }

    /**
     * Updates the hash value using the specified <code>byte[]</code>.
     */
    public default F update(byte[] input) {
        return update(input, 0, input.length);
    }

    /**
     * Updates the hash value using a part of the specified <code>byte[]</code>.
     */
    public F update(byte[] input, int offset, int length);

    /**
     * Compute the final hash value and return it as a new <code>byte[]</code>.
     * <p>
     * This instance is {@linkplain #reset() reset} after calling this method.
     */
    public default byte[] finish() {
        final byte[] result = new byte[getHashLength()];
        finish(result, 0);
        return result;
    };

    /**
     * Compute the final hash value and write it in the specified
     * <code>byte[]</code> at the given offset.
     * <p>
     * This instance is {@linkplain #reset() reset} after calling this method.
     *
     * @throws IllegalArgumentException If the buffer was not big enough.
     */
    public void finish(byte[] output, int offset)
    throws IllegalArgumentException;

    /**
     * Reset this instance to its original construction state, discarding all
     * the data it was {@linkplain #update(byte[], int, int) updated} with.
     */
    public F reset();

}
