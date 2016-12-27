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

import static java.lang.String.format;

import java.util.IllegalFormatException;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Check {

    private Check() {
        throw new IllegalStateException("Do not construct");
    }

    /* ====================================================================== */

    public static <T> T notNull(T object) {
        if (object != null) return object;
        throw new NullPointerException("Null object");
    }

    public static <T> T notNull(T object, String message) {
        if (object != null) return object;
        throw new NullPointerException(message == null ? "Null object" : message);
    }

    public static <T> T notNull(T object, String format, Object... arguments) {
        if (object != null) return object;
        if (format == null) throw new NullPointerException("Null object");
        try {
            throw new NullPointerException(format(format, arguments));
        } catch (IllegalFormatException exception) {
            throw (NullPointerException) new NullPointerException("Null object").initCause(exception);
        }
    }

    public static <T, E extends Exception> T notNull(T object, E exception)
    throws E {
        if (object != null) return object;
        if (exception != null) throw exception;
        throw new NullPointerException("Null object");
    }

    /* ---------------------------------------------------------------------- */

    public static <T> T notNull(Supplier<T> supplier) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notNull(supplier.get());
    }

    public static <T> T notNull(Supplier<T> supplier, String message) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notNull(supplier.get(), message);
    }

    public static <T> T notNull(Supplier<T> supplier, String format, Object... arguments) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notNull(supplier.get(), format, arguments);
    }

    public static <T, E extends Exception> T notNull(Supplier<T> supplier, E exception)
    throws E {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notNull(supplier.get(), exception);
    }

    /* ====================================================================== */

    public static <T> T check(T argument, boolean test) {
        if (test) return argument;
        throw new IllegalArgumentException("Invalid argument");
    }

    public static <T> T check(T argument, boolean test, String message) {
        if (test) return argument;
        if (message == null) throw new IllegalArgumentException("Invalid argument");
        throw new IllegalArgumentException(message);
    }

    public static <T> T check(T argument, boolean test, String format, Object... arguments) {
        if (test) return argument;
        try {
            throw new IllegalArgumentException(format(format, arguments));
        } catch (IllegalFormatException exception) {
            throw new IllegalArgumentException("Invalid argument", exception);
        }
    }

    public static <T, E extends Exception> T check(T argument, boolean test, E exception)
    throws E {
        if (test) return argument;
        if (exception != null) throw exception;
        throw new IllegalArgumentException("Invalid argument");
    }

    /* ---------------------------------------------------------------------- */

    public static <T> T check(Supplier<T> supplier, boolean test) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return check(supplier.get(), test);
    }

    public static <T> T check(Supplier<T> supplier, boolean test, String message) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return check(supplier.get(), test);
    }

    public static <T> T check(Supplier<T> supplier, boolean test, String format, Object... arguments) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return check(supplier.get(), test);
    }

    public static <T, E extends Exception> T check(Supplier<T> supplier, boolean test, E exception)
    throws E {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return check(supplier.get(), test);
    }

    /* ---------------------------------------------------------------------- */

    public static <T> T check(T argument, BooleanSupplier test) {
        if (test == null) throw new NullPointerException("Null test");
        return check(argument, test.getAsBoolean());
    }

    public static <T> T check(T argument, BooleanSupplier test, String message) {
        if (test == null) throw new NullPointerException("Null test");
        return check(argument, test.getAsBoolean(), message);
    }

    public static <T> T check(T argument, BooleanSupplier test, String format, Object... arguments) {
        if (test == null) throw new NullPointerException("Null test");
        return check(argument, test.getAsBoolean(), format, arguments);
    }

    public static <T, E extends Exception> T check(T argument, BooleanSupplier test, E exception)
    throws E {
        if (test == null) throw new NullPointerException("Null test");
        return check(argument, test.getAsBoolean(), exception);
    }

    /* ---------------------------------------------------------------------- */

    public static <T> T check(Supplier<T> supplier, BooleanSupplier test) {
        if (test == null) throw new NullPointerException("Null test");
        return check(supplier, test.getAsBoolean());
    }

    public static <T> T check(Supplier<T> supplier, BooleanSupplier test, String message) {
        if (test == null) throw new NullPointerException("Null test");
        return check(supplier, test.getAsBoolean(), message);
    }

    public static <T> T check(Supplier<T> supplier, BooleanSupplier test, String format, Object... arguments) {
        if (test == null) throw new NullPointerException("Null test");
        return check(supplier, test.getAsBoolean(), format, arguments);
    }

    public static <T, E extends Exception> T check(Supplier<T> supplier, BooleanSupplier test, E exception)
    throws E {
        if (test == null) throw new NullPointerException("Null test");
        return check(supplier, test.getAsBoolean(), exception);
    }

    /* ---------------------------------------------------------------------- */

    public static <T> T check(T argument, Predicate<T> predicate) {
        if (predicate == null) throw new NullPointerException("Null predicate");
        return check(argument, predicate.test(argument));
    }

    public static <T> T check(T argument, Predicate<T> predicate, String message) {
        if (predicate == null) throw new NullPointerException("Null predicate");
        return check(argument, predicate.test(argument), message);
    }

    public static <T> T check(T argument, Predicate<T> predicate, String format, Object... arguments) {
        if (predicate == null) throw new NullPointerException("Null predicate");
        return check(argument, predicate.test(argument), format, arguments);
    }

    public static <T, E extends Exception> T check(T argument, Predicate<T> predicate, E exception)
    throws E {
        if (predicate == null) throw new NullPointerException("Null predicate");
        return check(argument, predicate.test(argument), exception);
    }

    /* ---------------------------------------------------------------------- */

    public static <T> T check(Supplier<T> supplier, Predicate<T> predicate) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        if (predicate == null) throw new NullPointerException("Null predicate");
        final T argument = supplier.get();
        return check(argument, predicate.test(argument));
    }

    public static <T> T check(Supplier<T> supplier, Predicate<T> predicate, String message) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        if (predicate == null) throw new NullPointerException("Null predicate");
        final T argument = supplier.get();
        return check(argument, predicate.test(argument), message);
    }

    public static <T> T check(Supplier<T> supplier, Predicate<T> predicate, String format, Object... arguments) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        if (predicate == null) throw new NullPointerException("Null predicate");
        final T argument = supplier.get();
        return check(argument, predicate.test(argument), format, arguments);
    }

    public static <T, E extends Exception> T check(Supplier<T> supplier, Predicate<T> predicate, E exception)
    throws E {
        if (supplier == null) throw new NullPointerException("Null supplier");
        if (predicate == null) throw new NullPointerException("Null predicate");
        final T argument = supplier.get();
        return check(argument, predicate.test(argument), exception);
    }

    /* ====================================================================== */

    public static String notEmpty(String string) {
        return check(string, notNull(string).length() > 0);
    }

    public static String notEmpty(String string, String message) {
        return check(string, notNull(string).length() > 0, message);
    }

    public static String notEmpty(String string, String format, Object... arguments) {
        return check(string, notNull(string).length() > 0, format, arguments);
    }

    public static <E extends Exception> String notEmpty(String string, E exception)
    throws E {
        return check(string, notNull(string).length() > 0, exception);
    }

    /* ---------------------------------------------------------------------- */

    public static String notEmpty(Supplier<String> supplier) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notEmpty(supplier.get());
    }

    public static String notEmpty(Supplier<String> supplier, String message) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notEmpty(supplier.get(), message);
    }

    public static String notEmpty(Supplier<String> supplier, String format, Object... arguments) {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notEmpty(supplier.get(), format, arguments);
    }

    public static <E extends Exception> String notEmpty(Supplier<String> supplier, E exception)
    throws E {
        if (supplier == null) throw new NullPointerException("Null supplier");
        return notEmpty(supplier.get(), exception);
    }

}
