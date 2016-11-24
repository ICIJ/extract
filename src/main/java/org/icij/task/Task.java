package org.icij.task;

public interface Task<T extends Option, U, R> {

	Options<T> options();

	T option(final String name);

	String description();

	R run() throws Exception;

	R run(final U arguments) throws Exception;
}
