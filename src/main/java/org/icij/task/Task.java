package org.icij.task;

public interface Task<T, U, R> {

	Options<T> options();

	Option<T> option(final String name);

	String description();

	R run() throws Exception;

	R run(final U arguments) throws Exception;
}
