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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.security.spec.DSAPrivateKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.pkcs.RSAPrivateKey;
import org.bouncycastle.asn1.x509.DSAParameter;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;

/**
 * Utility factory/class for contents of {@linkplain PEMEntry PEM entries}.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
class PEMFactory {

    private final Provider provider;

    PEMFactory() {
        provider = null;
    }

    PEMFactory(String provider)
    throws NoSuchProviderException {
        if (provider == null) throw new NullPointerException("Null provider name");
        this.provider = Security.getProvider(provider);
        if (this.provider == null) throw new NoSuchProviderException("Unknown provider \"" + provider + "\"");
    }

    PEMFactory(Provider provider) {
        if (provider == null) throw new NullPointerException("Null provider");
        this.provider = provider;
    }

    /* ====================================================================== */

    private KeyFactory getRSAKeyFactory()
    throws NoSuchAlgorithmException {
        return provider == null ? KeyFactory.getInstance("RSA") :
               KeyFactory.getInstance("RSA", provider);
    }

    private KeyFactory getDSAKeyFactory()
    throws NoSuchAlgorithmException {
        return provider == null ? KeyFactory.getInstance("DSA") :
               KeyFactory.getInstance("DSA", provider);
    }

    private CertificateFactory getCertificateFactory()
    throws CertificateException {
        return provider == null ? CertificateFactory.getInstance("X.509") :
            CertificateFactory.getInstance("X.509", provider);
    }

    /* ====================================================================== */

    public PrivateKey getPrivateKey(PrivateKeyInfo keyInfo)
    throws NoSuchAlgorithmException, InvalidKeyException, InvalidKeySpecException {

        final Object algorithmId = keyInfo.getPrivateKeyAlgorithm().getAlgorithm();
        final ASN1Encodable encodable;
        try {
            encodable = keyInfo.parsePrivateKey();
        } catch (IOException exception) {
            throw new InvalidKeyException("Unable to parse private key structure", exception);
        }

        /* DSA keys */
        if (algorithmId.equals(X9ObjectIdentifiers.id_dsa)) {
            final ASN1Encodable encodedParams = keyInfo.getPrivateKeyAlgorithm().getParameters();
            final DSAParameter params = DSAParameter.getInstance(encodedParams);
            final BigInteger x = ASN1Integer.getInstance(encodable).getValue();
            return getDSAKeyFactory().generatePrivate(new DSAPrivateKeySpec(
                        x,
                        params.getP(),
                        params.getQ(),
                        params.getG()));
        }

        /* RSA keys */
        if (algorithmId.equals(PKCSObjectIdentifiers.rsaEncryption)) {
            final RSAPrivateKey privateKey = RSAPrivateKey.getInstance(encodable);
            return getRSAKeyFactory().generatePrivate(new RSAPrivateCrtKeySpec(
                        privateKey.getModulus(),
                        privateKey.getPublicExponent(),
                        privateKey.getPrivateExponent(),
                        privateKey.getPrime1(),
                        privateKey.getPrime2(),
                        privateKey.getExponent1(),
                        privateKey.getExponent2(),
                        privateKey.getCoefficient()));
        }

        /* Others? */
        throw new NoSuchAlgorithmException("Unsupported algorithm for private key: " + algorithmId);

    }

    public PublicKey getPublicKey(SubjectPublicKeyInfo keyInfo)
    throws NoSuchAlgorithmException, InvalidKeyException, InvalidKeySpecException {

        final Object algorithmId = keyInfo.getAlgorithm().getAlgorithm();
        final byte[] encoded;
        try {
            encoded = keyInfo.getEncoded();
        } catch (IOException exception) {
            throw new InvalidKeyException("Unable to get encoded key", exception);
        }

        /* DSA keys */
        if (algorithmId.equals(X9ObjectIdentifiers.id_dsa)) {
            return getDSAKeyFactory().generatePublic(new X509EncodedKeySpec(encoded));
        }

        /* RSA keys */
        if (algorithmId.equals(PKCSObjectIdentifiers.rsaEncryption)) {
            return getRSAKeyFactory().generatePublic(new X509EncodedKeySpec(encoded));
        }

        /* Others? */
        throw new NoSuchAlgorithmException("Unsupported algorithm for private key: " + algorithmId);
    }

    public X509Certificate getCertificate(X509CertificateHolder holder)
    throws CertificateException {
        try {
            final byte[] encoded = holder.getEncoded();
            final ByteArrayInputStream stream = new ByteArrayInputStream(encoded);
            return (X509Certificate) getCertificateFactory().generateCertificate(stream);
        } catch (IOException exception) {
            throw new CertificateException("Unable to get encoded certificate", exception);
        }
    }

    public X509CRL getCRL(X509CRLHolder holder)
    throws CertificateException, CRLException {
        try {
            final byte[] encoded = holder.getEncoded();
            final ByteArrayInputStream stream = new ByteArrayInputStream(encoded);
            return (X509CRL) getCertificateFactory().generateCRL(stream);
        } catch (IOException exception) {
            throw new CertificateException("Unable to get encoded certificate", exception);
        }
    }

}
