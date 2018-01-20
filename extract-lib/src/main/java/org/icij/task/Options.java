package org.icij.task;

import java.util.*;
import java.util.function.Function;

import static java.lang.String.join;
import static java.util.stream.Collectors.toList;

public class Options<T> implements Iterable<Option<T>> {
    protected final Map<String, Option<T>> map = new HashMap<>();

    public <R> Optional<R> ifPresent(final String name, final Function<Option<T>, Optional<R>> function) {
        return map.containsKey(name) ? function.apply(get(name)) : Optional.empty();
    }

    public Option<T> get(final String name) {
        return map.get(name);
    }

    public Option<T> get(final Option<T> option) {
        return map.get(option.name());
    }

    public Options<T> add(final Option<T> option) {
        map.put(option.name(), option);
        return this;
    }

    public Option<T> add(final String name, final Function<Option<T>, OptionParser<T>> parser) {
        final Option<T> option = new Option<>(name, parser);

        add(option);
        return option;
    }

    public Option<T> add(final org.icij.task.annotation.Option option, final Function<Option<T>, OptionParser<T>>
            parser) {
        return add(option.name(), parser).describe(option.description())
                .parameter(option.parameter())
                .code(option.code());
    }

    public void add(final org.icij.task.annotation.OptionsClass optionsClass, final Function<Option<T>, OptionParser<T>>
            parser) {
        for (org.icij.task.annotation.Option option : optionsClass.value()
                .getAnnotationsByType(org.icij.task.annotation.Option.class)) {
            add(option, parser);
        }

        // Recursively import other Options from OptionsClass annotations.
        for (org.icij.task.annotation.OptionsClass otherClass : optionsClass.value()
                .getAnnotationsByType(org.icij.task.annotation.OptionsClass.class)) {
            add(otherClass, parser);
        }
    }

    public static Options<String> from(final Map<String, String> stringProperties) {
        Options<String> options = new Options<>();
        stringProperties.forEach(
                (key, value) -> options.add(new Option<>(key, StringOptionParser::new).update(value))
        );
        return options;
    }

    public static Options<String> from(final Properties stringProperties) {
        Map<String, String> map = new HashMap<>();
        stringProperties.forEach((key, value) -> map.put((String)key, (String)value));
        return from(map);
    }

    @Override
    public Iterator<Option<T>> iterator() {
        return new OptionsIterator<>(map);
    }

    @Override
    public String toString() {
        return "{" + join(",", map.values().stream().map(Object::toString).collect(toList())) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Options<?> options = (Options<?>) o;
        return Objects.equals(map, options.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

}
