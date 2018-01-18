package org.icij.task;

import java.lang.reflect.AnnotatedElement;

public abstract class DefaultTask<R> implements Task<String, String[], R> {

	protected static Options<String> options(final Class<? extends DefaultTask> taskClass) {
		final Options<String> options = new Options<>();

		for (org.icij.task.annotation.Option option : taskClass
				.getAnnotationsByType(org.icij.task.annotation.Option.class)) {
			options.add(option, StringOptionParser::new);
		}

		for (org.icij.task.annotation.OptionsClass otherClass : taskClass
				.getAnnotationsByType(org.icij.task.annotation.OptionsClass.class)) {
			options.add(otherClass, StringOptionParser::new);
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
	public R call(final String[] arguments) throws Exception {
		return call();
	}
}
