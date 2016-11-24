package org.icij.task;

import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

import java.lang.reflect.AnnotatedElement;

public abstract class DefaultTask<R> implements org.icij.task.Task<StringOption, String[], R> {

	protected static StringOptions options(final Class<? extends DefaultTask> taskClass) {
		final StringOptions options = new StringOptions();
		final Option[] descriptions = taskClass.getAnnotationsByType(Option.class);

		for (Option description : descriptions) {
			final String code = description.code();
			final StringOption option = options.add(description.name())
					.describe(description.description())
					.parameter(description.parameter());

			if (!code.isEmpty()) {
				option.code(code.toCharArray()[0]);
			}
		}

		return options;
	}

	protected final StringOptions options = DefaultTask.options(this.getClass());

	public StringOptions options() {
		return options;
	}

	@Override
	public StringOption option(final String name) {
		return options.get(name);
	}

	@Override
	public String description() {
		final AnnotatedElement element = getClass();
		final Class<Task> c = Task.class;

		if (!element.isAnnotationPresent(c)) {
			return null;
		}

		return element.getAnnotation(c).value();
	}

	@Override
	public R run(final String[] arguments) throws Exception {
		return run();
	}
}
