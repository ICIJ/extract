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

import static org.usrz.libs.crypto.hash.Hash.SHA1;
import static org.usrz.libs.utils.codecs.HexCodec.HEX;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.KeyStoreSpi;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CRLException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.x500.X500Principal;

/**
 * A JCE security {@link KeyStoreSpi} adding support for {@link KeyStore}s
 * initialized from PEM-encoded,
 * <a href="http://www.openssl.org/">OpenSSL</a>-style files.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public class PEMKeyStoreSpi extends KeyStoreSpi {

    private static final Logger logger = Logger.getLogger(PEMKeyStoreSpi.class.getName());

    private final ConcurrentHashMap<String, Holder> holders;

    public PEMKeyStoreSpi() {
        holders = new ConcurrentHashMap<>();
    }

    /* ====================================================================== *
     * LOADING                                                                *
     * ====================================================================== */

    @Override
    public void engineLoad(InputStream stream, char[] password)
    throws IOException, NoSuchAlgorithmException, CertificateException {

        final Map<X500Principal, X509Certificate> certificates = new HashMap<>();
        final PEMReader reader = new PEMReader(stream);
        try {
            PEMEntry<?> entry = null;

            while ((entry = reader.read()) != null) {
                if (logger.isLoggable(Level.FINER)) logger.finer("Found " + entry);

                if (Certificate.class.isAssignableFrom(entry.getType())) {
                    final X509Certificate certificate = (X509Certificate) entry.get(password);
                    certificates.put(certificate.getSubjectX500Principal(), certificate);

                    final PublicKey publicKey = certificate.getPublicKey();
                    final Holder holder = newHolder(publicKey, holders);
                    holder.certificate = certificate;
                    if (logger.isLoggable(Level.FINE))
                        logger.fine("Added certificate for alias " + holder.alias +
                                    " with principal " + certificate.getSubjectX500Principal());
                    continue;
                }

                if (KeyPair.class.isAssignableFrom(entry.getType())) {
                    @SuppressWarnings("unchecked")
                    final PEMEntry<KeyPair> keyPairEntry = (PEMEntry<KeyPair>) entry;
                    final KeyPair keyPair = keyPairEntry.get(password);
                    final PublicKey publicKey = keyPair.getPublic();
                    final Holder holder = newHolder(publicKey, holders);
                    if (holder.entry != null)
                        throw new IllegalStateException("Duplicate key " + holder.alias + " found in PEM file");

                    holder.entry = entry;
                    if (logger.isLoggable(Level.FINE))
                        logger.fine("Added key pair for alias " + holder.alias);
                    continue;
                }

                if (PublicKey.class.isAssignableFrom(entry.getType())) {
                    final PublicKey publicKey = (PublicKey) entry.get(password);
                    final Holder holder = newHolder(publicKey, holders);
                    if (holder.entry != null)
                        throw new IllegalStateException("Duplicate key " + holder.alias + " found in PEM file");

                    holder.entry = entry;
                    if (logger.isLoggable(Level.FINE))
                        logger.fine("Added public key for alias " + holder.alias);
                    continue;
                }

                logger.warning("Ignoring entry for " + entry.getType().getName());
            }

        } catch (CRLException | InvalidKeyException | InvalidKeySpecException exception) {
            throw new IllegalArgumentException("Unable to decrypt data", exception);
        } finally {
            reader.close();
        }

        /* Build certificate chains */
        for (Holder holder: holders.values()) {

            /* Our certificate */
            X509Certificate certificate = holder.certificate;
            if (certificate == null) continue;

            /* Start building a chain */
            if (logger.isLoggable(Level.FINER))
                logger.finer("Building chain for " + certificate.getSubjectX500Principal());

            /* Our chain, always containing the first certificate */
            final List<X509Certificate> chain = new ArrayList<>();
            chain.add(certificate);

            /* Look for the issuer */
            X509Certificate issuer = certificates.get(certificate.getIssuerX500Principal());
            while ((issuer != null) && (!issuer.equals(certificate))) {

                if (logger.isLoggable(Level.FINER))
                    logger.finer("Issuer for " + certificate.getSubjectX500Principal() + " is " + issuer.getSubjectX500Principal());

                /* Verify that we actually have a proper signature */
                try {
                    certificate.verify(issuer.getPublicKey());
                } catch (InvalidKeyException | NoSuchProviderException | SignatureException exception) {
                    final String message = "Unable to verify certificate " + certificate.getSubjectX500Principal() +
                                           " with issuer " + issuer.getSubjectX500Principal();
                    throw new CertificateException(message, exception);
                }

                /* Add the issuer certificate to the chain and repeat */
                chain.add(issuer);
                certificate = issuer;
                issuer = certificates.get(certificate.getIssuerX500Principal());
            }

            holder.chain = chain.toArray(new X509Certificate[chain.size()]);
        }
    }

    /* ====================================================================== *
     * KEYSTORE SUPPORTED METHODS                                             *
     * ====================================================================== */

    @Override
    public Date engineGetCreationDate(String alias) {
        /* Return the epoch if the entry exists, PEM doesn't store dates */
        return holders.containsKey(alias) ? new Date(0) : null;
    }

    @Override
    public boolean engineContainsAlias(String alias) {
        return holders.containsKey(alias);
    }

    @Override
    public Enumeration<String> engineAliases() {
        return Collections.enumeration(holders.keySet());
    }

    @Override
    public int engineSize() {
        return holders.size();
    }

    @Override
    public String engineGetCertificateAlias(Certificate certificate) {
        final String alias = alias(certificate.getPublicKey());
        return holders.containsKey(alias) ? alias : null;
    }

    /* ====================================================================== */

    @Override
    public boolean engineIsCertificateEntry(String alias) {
        final Holder holder = holders.get(alias);
        return holder == null ? false : holder.certificate != null;
    }

    @Override
    public boolean engineIsKeyEntry(String alias) {
        final Holder holder = holders.get(alias);
        return holder == null ? false : holder.entry != null;
    }

    /* ====================================================================== */

    @Override
    public X509Certificate engineGetCertificate(String alias) {
        final Holder holder = holders.get(alias);
        return holder == null ? null : holder.certificate;
    }

    @Override
    public Certificate[] engineGetCertificateChain(String alias) {
        final Holder holder = holders.get(alias);
        return holder == null ? null : holder.chain;
    }

    @Override
    public Key engineGetKey(String alias, char[] password)
    throws NoSuchAlgorithmException, UnrecoverableKeyException {
        try {
            final Holder holder = holders.get(alias);
            if ((holder == null) || (holder.entry == null)) return null;
            final Object object = holder.entry.get(password);
            if (object instanceof Key) return (Key) object;
            if (object instanceof KeyPair) {
                final KeyPair keyPair = (KeyPair) object;
                final PrivateKey privateKey = keyPair.getPrivate();
                return privateKey != null ? privateKey : keyPair.getPublic();
            }
            return null;
        } catch (GeneralSecurityException exception) {
            final String message = "Invalid password for key alias " + alias;
            final Throwable throwable = new UnrecoverableKeyException(message);
            throw (UnrecoverableKeyException) throwable.initCause(exception);
        }
    }

    /* ====================================================================== *
     * UNSUPPORTED METHODS                                                    *
     * ====================================================================== */

    @Override
    public void engineStore(OutputStream stream, char[] password)
    throws IOException, NoSuchAlgorithmException, CertificateException {
        throw new IOException("PEM file can not be saved");
    }

    @Override
    public void engineDeleteEntry(String alias)
    throws KeyStoreException {
        throw new KeyStoreException("PEM key store is read-only");
    }

    @Override
    public void engineSetKeyEntry(String alias, byte[] key, Certificate[] chain)
    throws KeyStoreException {
        throw new KeyStoreException("PEM key store is read-only");
    }

    @Override
    public void engineSetKeyEntry(String alias, Key key, char[] password, Certificate[] chain)
    throws KeyStoreException {
        throw new KeyStoreException("PEM key store is read-only");
    }

    @Override
    public void engineSetCertificateEntry(String alias, Certificate cert)
    throws KeyStoreException {
        throw new KeyStoreException("PEM key store is read-only");
    }

    /* ====================================================================== *
     * INTERNAL METHODS                                                       *
     * ====================================================================== */

    private static Holder newHolder(PublicKey key, ConcurrentHashMap<String, Holder> holders) {
        return holders.computeIfAbsent(alias(key), (alias) -> new Holder(alias));
    }

    private static String alias(PublicKey key) {
        return key == null ? null : HEX.encode(SHA1.digest().update(key.getEncoded()).finish());
    }

    private static class Holder {
        private final String alias;
        private X509Certificate certificate;
        private Certificate[] chain;
        private PEMEntry<?> entry;

        private Holder(String alias) {
            if (alias == null) throw new NullPointerException("Null alias");
            this.alias = alias;
        }
    }

}
