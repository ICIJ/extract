package org.icij.events;

public interface Listener {

	void step(final Monitorable monitorable, final Object arg);

	void step(final Object arg);

	void steps(final int total);
}
