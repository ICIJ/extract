package org.icij.task;

import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

import java.lang.reflect.AnnotatedElement;

public abstract class DefaultTask<R> implements org.icij.task.Task<DefaultOption, String[], R> {

	protected static DefaultOption.Set options(final Class<? extends DefaultTask> taskClass) {
		final DefaultOption.Set options = new DefaultOption.Set();
		final Option[] descriptions = taskClass.getAnnotationsByType(Option.class);

		for (Option description : descriptions) {
			final String code = description.code();
			final DefaultOption option = options.add(description.name())
					.describe(description.description())
					.parameter(description.parameter());

			if (!code.isEmpty()) {
				option.code(code.toCharArray()[0]);
			}
		}

		return options;
	}

	protected final DefaultOption.Set options = DefaultTask.options(this.getClass());

	public DefaultOption.Set options() {
		return options;
	}

	@Override
	public DefaultOption option(final String name) {
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
