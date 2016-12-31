package org.icij.task;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

public interface OptionParser<V> {

	Optional<Duration> asDuration();

	Optional<Path> asPath();

	Optional<URI> asURI();

	Optional<Integer> asInteger();

	Optional<Boolean> asBoolean();

	Optional<Charset> asCharset();

	boolean isOn();

	boolean isOff();

	<E extends Enum<E>> Optional<E> asEnum(final Function<V, E> valueOf);
}
