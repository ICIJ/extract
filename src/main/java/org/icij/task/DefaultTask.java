package org.icij.task;

import java.lang.reflect.AnnotatedElement;

public abstract class DefaultTask<R> implements Task<String, String[], R> {

	protected static Options<String> options(final Class<? extends DefaultTask> taskClass) {
		final Options<String> options = new StringOptions();
		final org.icij.task.annotation.Option[] descriptions = taskClass
				.getAnnotationsByType(org.icij.task.annotation.Option.class);

		for (org.icij.task.annotation.Option description : descriptions) {
			final String code = description.code();
			final org.icij.task.Option<String> option = options.add(description.name())
					.describe(description.description())
					.parameter(description.parameter());

			if (!code.isEmpty()) {
				option.code(code.toCharArray()[0]);
			}
		}

		return options;
	}

	protected final Options<String> options = DefaultTask.options(this.getClass());

	@Override
	public Options<String> options() {
		return options;
	}

	@Override
	public Option<String> option(final String name) {
		return options.get(name);
	}

	@Override
	public String description() {
		final AnnotatedElement element = getClass();
		final Class<org.icij.task.annotation.Task> c = org.icij.task.annotation.Task.class;

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
