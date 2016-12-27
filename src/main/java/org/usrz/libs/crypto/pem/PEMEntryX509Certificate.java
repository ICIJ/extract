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

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.bouncycastle.cert.X509CertificateHolder;

/**
 * A {@linkplain PEMEntry PEM entry} wrapping an
 * {@linkplain X509Certificate X.509 certificate}.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public final class PEMEntryX509Certificate extends PEMEntry<X509Certificate> {

    private final X509Certificate certificate;

    /* Restrict construction of instances to this package */
    PEMEntryX509Certificate(PEMFactory factory, X509CertificateHolder holder)
    throws CertificateException {
        super(X509Certificate.class, false);
        certificate = factory.getCertificate(holder);
    }

    @Override
    public X509Certificate get() {
        return certificate;
    }


}
