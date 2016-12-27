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

import java.security.Provider;

/**
 * A JCE security {@link Provider} adding support for PEM-encoded
 * <a href="http://www.openssl.org/">OpenSSL</a>-style files.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public class PEMProvider extends Provider {

    public PEMProvider() {
        super(PEMProvider.class.getSimpleName(), 1.0, "OpenSSL PEM keys and certificates");
        put("KeyStore.PEM", PEMKeyStoreSpi.class.getName());
    }

}
