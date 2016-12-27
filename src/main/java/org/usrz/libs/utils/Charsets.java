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
package org.usrz.libs.utils;

import java.nio.charset.Charset;

/**
 * A collection of {@link Charset} instances.
 *
 * @author <a href="mailto:pier@usrz.com">Pier Fumagalli</a>
 */
public final class Charsets {

    /* Deny constructions */
    private Charsets() { throw new IllegalStateException(); }

    /** The <i>US-ASCII</i> character set. */
    public static final Charset ASCII = Charset.forName("US-ASCII");
    /** The <i>BIG5</i> character set. */
    public static final Charset BIG5 = Charset.forName("Big5");
    /** The <i>EUC_JP</i> character set. */
    public static final Charset EUC_JP = Charset.forName("EUC-JP");
    /** The <i>EUC_KR</i> character set. */
    public static final Charset EUC_KR = Charset.forName("EUC-KR");
    /** The <i>EUC_TW</i> character set. */
    public static final Charset EUC_TW = Charset.forName("EUC-TW");
    /** The <i>ISO2022_CN</i> character set. */
    public static final Charset ISO2022_CN = Charset.forName("ISO-2022-CN");
    /** The <i>ISO2022_JP</i> character set. */
    public static final Charset ISO2022_JP = Charset.forName("ISO-2022-JP");
    /** The <i>ISO2022_KR</i> character set. */
    public static final Charset ISO2022_KR = Charset.forName("ISO-2022-KR");
    /** The <i>ISO8859_1</i> character set. */
    public static final Charset ISO8859_1 = Charset.forName("ISO-8859-1");
    /** The <i>ISO8859_2</i> character set. */
    public static final Charset ISO8859_2 = Charset.forName("ISO-8859-2");
    /** The <i>ISO8859_3</i> character set. */
    public static final Charset ISO8859_3 = Charset.forName("ISO-8859-3");
    /** The <i>ISO8859_4</i> character set. */
    public static final Charset ISO8859_4 = Charset.forName("ISO-8859-4");
    /** The <i>ISO8859_5</i> character set. */
    public static final Charset ISO8859_5 = Charset.forName("ISO-8859-5");
    /** The <i>ISO8859_6</i> character set. */
    public static final Charset ISO8859_6 = Charset.forName("ISO-8859-6");
    /** The <i>ISO8859_7</i> character set. */
    public static final Charset ISO8859_7 = Charset.forName("ISO-8859-7");
    /** The <i>ISO8859_8</i> character set. */
    public static final Charset ISO8859_8 = Charset.forName("ISO-8859-8");
    /** The <i>ISO8859_9</i> character set. */
    public static final Charset ISO8859_9 = Charset.forName("ISO-8859-9");
    /** The <i>ISO8859_13</i> character set. */
    public static final Charset ISO8859_13 = Charset.forName("ISO-8859-13");
    /** The <i>ISO8859_15</i> character set. */
    public static final Charset ISO8859_15 = Charset.forName("ISO-8859-15");
    /** The <i>SHIFT_JIS</i> character set. */
    public static final Charset SHIFT_JIS = Charset.forName("Shift_JIS");
    /** The <i>UTF8</i> character set. */
    public static final Charset UTF8 = Charset.forName("UTF-8");
    /** The <i>UTF16</i> character set. */
    public static final Charset UTF16 = Charset.forName("UTF-16");
    /** The <i>UTF16BE</i> character set. */
    public static final Charset UTF16BE = Charset.forName("UTF-16BE");
    /** The <i>UTF16LE</i> character set. */
    public static final Charset UTF16LE = Charset.forName("UTF-16LE");
    /** The <i>UTF32</i> character set. */
    public static final Charset UTF32 = Charset.forName("UTF-32");
    /** The <i>UTF32BE</i> character set. */
    public static final Charset UTF32BE = Charset.forName("UTF-32BE");
    /** The <i>UTF32LE</i> character set. */
    public static final Charset UTF32LE = Charset.forName("UTF-32LE");

}
