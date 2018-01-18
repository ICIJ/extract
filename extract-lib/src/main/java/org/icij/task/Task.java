package org.icij.task;

import java.util.concurrent.Callable;

public interface Task<T, U, R> extends Callable<R> {

	Options<T> options();

	Option<T> option(final String name);

	String description();

	R call(final U arguments) throws Exception;
}
