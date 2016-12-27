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

import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;

import org.bouncycastle.cert.X509CRLHolder;

public class PEMEntryCRL extends PEMEntry<X509CRL> {

    private final X509CRL crl;

    /* Restrict construction of instances to this package */
    PEMEntryCRL(PEMFactory factory, X509CRLHolder holder)
    throws CRLException, CertificateException {
        super(X509CRL.class, false);
        crl = factory.getCRL(holder);
    }

    @Override
    public X509CRL get() {
        return crl;
    }

}
