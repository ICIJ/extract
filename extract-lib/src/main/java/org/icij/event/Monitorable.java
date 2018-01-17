package org.icij.event;

public interface Monitorable {

	void addListener(final Listener listener);

	void deleteListener(final Listener listener);

	void deleteListeners();

	int countListeners();
}
