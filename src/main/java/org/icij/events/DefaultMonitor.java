package org.icij.events;

import java.util.Vector;

public class DefaultMonitor implements Monitor {

	private Vector<Listener> listeners;

	public DefaultMonitor() {
		listeners = new Vector<>();
	}

	@Override
	public synchronized void addListener(final Listener listener) {
		if (!listeners.contains(listener)) {
			listeners.addElement(listener);
		}
	}

	@Override
	public synchronized void deleteListener(final Listener listener) {
		listeners.removeElement(listener);
	}

	@Override
	public void notifyListeners() {
		notifyListeners(null);
	}

	@Override
	public void notifyListeners(final Object arg) {
		Object[] listeners;

		synchronized (this) {
			listeners = this.listeners.toArray();
		}

		for (int i = listeners.length - 1; i >= 0; i--) {
			((Listener) listeners[i]).step(this, arg);
		}
	}

	@Override
	public void hintRemaining(final int remaining) {
		Object[] listeners;

		synchronized (this) {
			listeners = this.listeners.toArray();
		}

		for (int i = listeners.length - 1; i >= 0; i--) {
			((Listener) listeners[i]).steps(remaining);
		}
	}

	@Override
	public synchronized void deleteListeners() {
		listeners.removeAllElements();
	}

	@Override
	public synchronized int countListeners() {
		return listeners.size();
	}
}
