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

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.logging.Logger;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;

/**
 * A reader for PEM-formatted files.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public class PEMReader implements Closeable {

    private static final Logger logger = Logger.getLogger(PEMReader.class.getName());

    private final PEMFactory factory;
    private final PEMParser parser;

    /**
     * Create a {@link PEMReader} loading from an {@link InputStream}.
     */
    public PEMReader(InputStream input) {
        if (input == null) throw new NullPointerException("Null input stream");

        factory = new PEMFactory();
        parser = new PEMParser(new InputStreamReader(input, US_ASCII));
    }

    /**
     * Create a {@link PEMReader} loading from a {@link Reader}.
     */
    public PEMReader(Reader reader) {
        if (reader == null) throw new NullPointerException("Null reader");

        factory = new PEMFactory();
        parser = new PEMParser(reader);
    }

    /**
     * Create a {@link PEMReader} loading from an {@link InputStream}.
     */
    public PEMReader(String provider, InputStream input)
    throws NoSuchProviderException {
        if (input == null) throw new NullPointerException("Null input stream");
        if (provider == null) throw new NullPointerException("Null provider");

        factory = new PEMFactory(provider);
        parser = new PEMParser(new InputStreamReader(input, US_ASCII));
    }

    /**
     * Create a {@link PEMReader} loading from a {@link Reader}.
     */
    public PEMReader(String provider, Reader reader)
    throws NoSuchProviderException {
        if (reader == null) throw new NullPointerException("Null reader");
        if (provider == null) throw new NullPointerException("Null provider");

        factory = new PEMFactory(provider);
        parser = new PEMParser(reader);
    }

    /**
     * Create a {@link PEMReader} loading from an {@link InputStream}.
     */
    public PEMReader(Provider provider, InputStream input) {
        if (input == null) throw new NullPointerException("Null input stream");
        if (provider == null) throw new NullPointerException("Null provider");

        factory = new PEMFactory(provider);
        parser = new PEMParser(new InputStreamReader(input, US_ASCII));
    }

    /**
     * Create a {@link PEMReader} loading from a {@link Reader}.
     */
    public PEMReader(Provider provider, Reader reader) {
        if (reader == null) throw new NullPointerException("Null reader");
        if (provider == null) throw new NullPointerException("Null provider");

        factory = new PEMFactory(provider);
        parser = new PEMParser(reader);
    }

    /* ====================================================================== */

    /**
     * Read a {@linkplain List list} of {@linkplain PEMEntry entries} from the
     * input specified at construction.
     */
    public PEMEntry<?> read()
    throws IOException, CertificateException, CRLException, InvalidKeyException,
           NoSuchAlgorithmException, InvalidKeySpecException {

        Object object = parser.readObject();
        while (object != null) {

            if (object instanceof X509CertificateHolder) {
                // X509 certificate
                return(new PEMEntryX509Certificate(factory, (X509CertificateHolder) object));

            } else if (object instanceof X509CRLHolder) {
                // X509 CRL
                return(new PEMEntryCRL(factory, (X509CRLHolder) object));

            } else if (object instanceof PEMEncryptedKeyPair) {
                // Encrypted private key
                return(new PEMEntryEncryptedKeyPair(factory, (PEMEncryptedKeyPair) object));

            } else if (object instanceof PEMKeyPair) {
                // Non-encrypted private key
                return(new PEMEntryKeyPair(factory, (PEMKeyPair) object));

            } else if (object instanceof PrivateKeyInfo) {
                // Private key (hopefully with public exponent)
                return(new PEMEntryKeyPair(factory, (PrivateKeyInfo) object));

            } else if (object instanceof SubjectPublicKeyInfo) {
                // Public Key
                return(new PEMEntryPublicKey(factory, (SubjectPublicKeyInfo) object));

            } else {
                // Huh? What's this? Warn and off to the next object!
                logger.warning("Unrecognized PEM object " + object.getClass().getName() + ", skipping...");
                object = parser.readObject();
                continue;
            }
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }
}
