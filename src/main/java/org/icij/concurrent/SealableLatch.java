package org.icij.concurrent;

/**
 * @since 1.0.0
 */
public interface SealableLatch {

	void signal();

	void await() throws InterruptedException;

	void seal();

	boolean isSealed();
}
