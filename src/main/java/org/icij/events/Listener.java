package org.icij.events;

public interface Listener {

	void notify(final Monitorable monitorable, final Object arg);

	void notify(final Object arg);

	void hintRemaining(final int total);
}
