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
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PEMCombiningInputStream extends InputStream {

    private static final Logger logger = Logger.getLogger(PEMCombiningInputStream.class.getName());

    private final InputStream[] inputs;
    private int position = 0;
    private boolean eof = false;

    public PEMCombiningInputStream(InputStream... inputs) {
        if (inputs == null) throw new NullPointerException("Null inputs");
        this.inputs = new InputStream[inputs.length];
        for (int x = 0; x < inputs.length; x ++) {
            if (inputs[x] == null) throw new NullPointerException("Null input at position " + x);
            this.inputs[x] = inputs[x];
        }
    }

    @Override
    public int read()
    throws IOException {

        /* Read until we have inputs available */
        while (position < inputs.length) {

            /* If we encountered an EOF, separate with '\n' */
            if (eof) {
                eof = false;
                return '\n';
            }

            /* Return the next byte or advance */
            final int data = inputs[position].read();
            if (data >= 0) return data;
            position++;
            eof = true;
        }

        /* Whops, no more files */
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len)
    throws IOException {

        /* Zero length array? Whops! */
        if (len == 0) return position < inputs.length ? 0 : -1;

        /* Read until we can have bytes, and have inputs */
        int tot = 0;
        while ((position < inputs.length) && (len > 0)) {

            /* Did we encounter an end of file? */
            if (eof) {
                eof = false;
                b[off] = '\n';
                tot ++;
                off ++;
                len --;
                continue;
            }

            /* Read until we can from the current input */
            int read = inputs[position].read(b, off, len);

            /* Whops, EOF? */
            if (read < 0) {
                position ++;
                eof = true;
                continue;
            }

            /* Adjust our numbers */
            tot += read;
            off += read;
            len -= read;
        }

        /* Return whatever we've read */
        return tot > 0 ? tot : -1;
    }

    @Override
    public int available()
    throws IOException {
        long available = 0;
        for (; position < inputs.length; position ++) {
            available += inputs[position].available();
        }
        if (available > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) available;
    }

    @Override
    public void close()
    throws IOException {
        position = inputs.length;
        for (InputStream input: inputs) try {
            input.close();
        } catch (Exception caught) {
            logger.log(Level.WARNING, "Exception closing wrapped stream");
        }
    }

}
