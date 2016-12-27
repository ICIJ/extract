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
package org.usrz.libs.utils.codecs;

/**
 * The {@link Codec} interface defines an object capable of converting between
 * <code>byte[]</code> and {@link String}s.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public interface Codec {

    /**
     * Encode the specified <code>byte[]</code> into a {@link String} according
     * to the algorithm implemented by this {@link Codec}.
     *
     * @param data The <code>byte[]</code> to encode.
     * @return A {@link String} containing the encoded data.
     * @throws NullPointerException If the specified data was <b>null</b>.
     */
    public String encode(byte[] data);

    /**
     * Encode a portion of the specified <code>byte[]</code> into a
     * {@link String} according to the algorithm implemented by this
     * {@link Codec}.
     *
     * @param data The <code>byte[]</code> to encode.
     * @return A {@link String} containing the encoded data.
     * @throws NullPointerException If the specified data was <b>null</b>.
     * @throws IndexOutOfBoundsException If the specified offset or length are
     *                                   invalid for the <code>byte[]</code>.
     */
    public String encode(byte[] data, int offset, int length);

    /**
     * Decode the specified {@link String} into a <code>byte[]</code> according
     * to the algorithm implemented by this {@link Codec}.
     *
     * @param data The {@link String} to decode.
     * @return A <code>byte[]</code> containing the decoded data.
     * @throws NullPointerException If the specified data was <b>null</b>.
     * @throws IllegalArgumentException If the data was not correctly encoded.
     */
    public byte[] decode(String data)
    throws IllegalArgumentException;

}
