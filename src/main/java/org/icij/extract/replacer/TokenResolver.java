/*
 * Copyright 2011-2015 PrimeFaces Extensions
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.icij.extract.replacer;

import java.io.Reader;
import java.io.IOException;

/**
 * Interface for resolving of tokens found via {@link TokenReplacingReader}.
 *
 * Based on original code by Oleg Varaksin (ovaraksin@googlemail.com), the
 * license of which is copied above. This version resolves tokens to
 * {@link Reader} instances.
 */
public interface TokenResolver {

	public Reader resolveToken(String token) throws IOException;
}
