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

import static org.usrz.libs.utils.Charsets.ASCII;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CRL;
import java.security.cert.CRLException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CollectionCertStoreParameters;
import java.security.spec.InvalidKeySpecException;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link CollectionCertStoreParameters} instance reading
 * {@linkplain Certificate} certificates and {@linkplain CRL CRLs}
 * from PEM files.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public class PEMCertStoreParameters
extends CollectionCertStoreParameters {

    /**
     * Create a new, <i>empty</i> {@link PEMCertStoreParameters} instance.
     */
    public PEMCertStoreParameters() {
        super(new HashSet<Object>());
    }

    /**
     * Create a new {@link PEMCertStoreParameters} instance reading
     * certificates and CRLs from the specified {@link InputStream}.
     */
    public PEMCertStoreParameters(InputStream input)
    throws IOException, InvalidKeyException, CRLException, CertificateException,
           NoSuchAlgorithmException, InvalidKeySpecException {
        super(new HashSet<Object>());
        read(input);
    }

    /**
     * Create a new {@link PEMCertStoreParameters} instance reading
     * certificates and CRLs from the specified {@link Reader}.
     */
    public PEMCertStoreParameters(Reader reader)
    throws IOException, InvalidKeyException, CRLException, CertificateException,
           NoSuchAlgorithmException, InvalidKeySpecException {
        super(new HashSet<Object>());
        read(reader);
    }

    /* ====================================================================== */

    /**
     * Read (potentially additional) certificates and CRLs from the
     * specified {@link InputStream}.
     *
     * <p>This method can be use to combine multiple PEM files.</p>
     */
    public void read(InputStream input)
    throws IOException, InvalidKeyException, CRLException, CertificateException,
           NoSuchAlgorithmException, InvalidKeySpecException {
        this.read(new InputStreamReader(input, ASCII));
    }

    /**
     * Read (potentially additional) certificates and CRLs from the
     * specified {@link Reader}.
     *
     * <p>This method can be use to combine multiple PEM files.</p>
     */
    public void read(Reader reader)
    throws IOException, InvalidKeyException, CRLException, CertificateException,
           NoSuchAlgorithmException, InvalidKeySpecException {

        final PEMReader pem = new PEMReader(reader);
        final Set<Object> set = new HashSet<>();

        try {
            PEMEntry<?> entry = null;
            while ((entry = pem.read()) != null) {
                if (Certificate.class.isAssignableFrom(entry.getType())) {
                    if (!entry.isEncrypted()) set.add(entry.get());
                    continue;
                }

                if (CRL.class.isAssignableFrom(entry.getType())) {
                    if (!entry.isEncrypted()) set.add(entry.get());
                    continue;
                }
            }

            @SuppressWarnings("unchecked")
            final Set<Object> collection = (Set<Object>) getCollection();
            collection.addAll(set);

        } finally {
            pem.close();
        }
    }
}
