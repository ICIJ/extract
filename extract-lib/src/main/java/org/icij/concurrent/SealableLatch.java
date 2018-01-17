package org.icij.concurrent;

public interface SealableLatch {
	void signal();
	void await() throws InterruptedException;
	void seal();
	boolean isSealed();
}
